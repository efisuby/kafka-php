<?php
/**
 * Kafka Client
 *
 * @category  Libraries
 * @package   Kafka
 * @author    Lorenzo Alberton <l.alberton@quipo.it>
 * @copyright 2012 Lorenzo Alberton
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link      http://sna-projects.com/kafka/
 */

/**
 * Zookeeper-based Kafka Consumer
 *
 * This is a sample implementation, there can be different strategies on how to consume
 * data from different brokers/partitions. Here the strategy is to read up to MAX_BATCH_SIZE
 * bytes from each partition before moving to the next. The order of brokers/partitions is
 * randomised in each loop to consume data from all queues in a more-or-less fair way.
 * An alternative strategy would be to round-robin the brokers/partitions, reading one message
 * from each; this strategy would be fairer, but way less efficient.
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
namespace Kafka;

class ZookeeperConsumer implements \Iterator
{
	/**
	 * @var Registry\Topic
	 */
	protected $topicRegistry;

	/**
	 * @var Registry\Broker
	 */
	protected $brokerRegistry;

	/**
	 * @var Registry\Offset
	 */
	protected $offsetRegistry;

	/**
	 * @var string
	 */
	protected $topic;

	/**
	 * @var integer
	 */
	protected $readBytes = 0;

	/**
	 * @var integer
	 */
	protected $socketTimeout = 0;

	/**
	 * @var integer
	 */
	protected $maxBatchSize = 20000000;

	/**
	 * @var \stdClass[]
	 */
	protected $iterators = array();

	/**
	 * @var integer
	 */
	protected $idx = 0;

	/**
	 * @var integer
	 */
	protected $nIterators = 0;

	/**
	 * @var boolean
	 */
	protected $hasMore = false;

	/**
	 * Create a new BatchedConsumer for a topic using the given TopicReigstry and OffsetRegistry.
	 *
	 * @param Registry\Topic  $topicRegistry  a registry for the discovery of topic partitions
	 * @param Registry\Broker $brokerRegistry a registry for the tracking of brokers
	 * @param Registry\Offset $offsetRegistry a registry for the tracking of the consumer offsets
	 * @param string          $topic          the topic to consume from
	 * @param integer         $maxBatchSize   maximum batch size (in bytes)
	 */
	public function __construct(
		Registry\Topic $topicRegistry,
		Registry\Broker $brokerRegistry,
		Registry\Offset $offsetRegistry,
		$topic,
		$maxBatchSize = 20000000
	) {
		$this->topicRegistry  = $topicRegistry;
		$this->brokerRegistry = $brokerRegistry;
		$this->offsetRegistry = $offsetRegistry;
		$this->topic          = $topic;
		$this->maxBatchSize   = $maxBatchSize;
	}

	/**
	 * Shuffle the internal iterators for each broker/partition
	 *
	 * @return void
	 */
	public function shuffle() {
		shuffle($this->iterators);
	}

	/**
	 * Advance the iterator's pointer
	 *
	 * @return Message
	 */
	public function next() {
		return $this->iterators[$this->idx]->messages->next();
	}

	/**
	 * Get the key for this item
	 *
	 * @return integer
	 */
	public function key() {
		return $this->iterators[$this->idx]->messages->key();
	}

	/**
	 * Get the current message
	 *
	 * @return mixed
	 */
	public function current() {
		return $this->iterators[$this->idx]->messages->current()->payload();
	}

	/**
	 * Check whether we have a valid iterator
	 *
	 * @return boolean
	 */
	public function valid() {
		while ($this->idx < $this->nIterators) {
			$it = $this->iterators[$this->idx];
			try {
				if (null === $it->messages) {
					$it->consumer = new SimpleConsumer($it->host, $it->port, $this->socketTimeout, $this->maxBatchSize);
					$newOffset = $it->offset + $it->uncommittedOffset;
					$request = new FetchRequest($this->topic, $it->partition, $newOffset, $this->maxBatchSize);
					$it->messages = $it->consumer->fetch($request);
					$it->messages->rewind();
				}
				if ($it->messages->valid()) {
					$this->hasMore = true;
					return true;
				}
				// we're done with the current broker/partition, count how much we've read so far and update the offsets
				$this->readBytes += $it->messages->validBytes();
				$it->uncommittedOffset += $it->messages->validBytes();
			} catch (Exception\EmptyQueue $e) {
				// no new data from this broker/partition
			}
			// reset the MessageSet iterator and move to the next
			$it->messages = null;
			$it->consumer->close();
			++$this->idx;
			if ($this->idx === $this->nIterators) {
				$this->idx = 0;
				// if we looped through all brokers/partitions and we did get data
				// from at least one of them, reset the iterator and do another loop
				if ($this->hasMore) {
					$this->hasMore = false;
				} else if ($this->getRemainingSize() > 1048576) {
					// we often get stuck, i.e. we fetch 0 bytes even if there is more data... keep trying
				} else {
					return false;
				}
			}
		}
		return false;
	}

	/**
	 * Return the number of bytes read so far
	 *
	 * @return integer
	 */
	public function getReadBytes() {
		if (0 == $this->nIterators) {
			return 0;
		}
		$it = $this->iterators[$this->idx];
		$readInCurrentPartition = isset($it->messages) ? $it->messages->validBytes() : 0;
		return $this->readBytes + $readInCurrentPartition;
	}

	/**
	 * Commit the kafka offsets for each broker/partition in ZooKeeper
	 *
	 * @return integer
	 */
	public function commitOffsets() {
		foreach ($this->iterators as $it) {
			$readBytes = $it->uncommittedOffset;
			if (null !== $it->messages) {
				$readBytes += $it->messages->validBytes();
			}
			if ($readBytes > 0) {
				$this->offsetRegistry->commit($this->topic, $it->broker, $it->partition, $it->offset + $readBytes);
				$it->uncommittedOffset = 0;
				$it->offset += $readBytes;
			}
		}
	}

	/**
	 * Resync invalid offsets to the first valid position
	 *
	 * @return integer Number of partitions/broker resync'ed
	 */
	public function resyncOffsets() {
		$nReset = 0;
		foreach ($this->iterators as $it) {
			$consumer = new SimpleConsumer($it->host, $it->port, $this->socketTimeout, $this->maxBatchSize);
			try {
				$newOffset = $it->offset + $it->uncommittedOffset;
				$request = new FetchRequest($this->topic, $it->partition, $newOffset, $this->maxBatchSize);
				$it->messages = $consumer->fetch($request);
			} catch (Exception\OffsetOutOfRange $e) {
				$offsets = $consumer->getOffsetsBefore($this->topic, $it->partition, SimpleConsumer::OFFSET_FIRST, 1);
				if (count($offsets) > 0) {
					$newOffset = $offsets[0];
					$this->offsetRegistry->commit($this->topic, $it->broker, $it->partition, $newOffset);
					$it->uncommittedOffset = 0;
					$it->offset = $newOffset;
					++$nReset;
				}
			}
		}
		return $nReset;
	}

	/**
	 * Get an approximate measure of the amount of data still to be consumed
	 *
	 * @return integer
	 */
	public function getRemainingSize() {
		try {
			if (0 == $this->nIterators) {
				$this->rewind();	// initialise simple consumers
			}
		} catch (Exception\InvalidTopic $e) {
			$logMsg = 'Invalid topic from ZookeeperConsumer::rewind(): Most likely cause is no topic yet as there is no data';
			error_log($logMsg);
		}
		$totalSize = 0;
		foreach ($this->iterators as $it) {
			$readBytes = $it->offset + $it->uncommittedOffset;
			if (null !== $it->messages) {
				$readBytes += $it->messages->validBytes();
			}
			$consumer = new SimpleConsumer($it->host, $it->port, $this->socketTimeout, $this->maxBatchSize);
			$offsets = $consumer->getOffsetsBefore($this->topic, $it->partition, SimpleConsumer::OFFSET_LAST, 1);
			if (count($offsets) > 0) {
				$remaining = $offsets[0] - $readBytes; // remaining bytes for this broker/partition
				if ($remaining > 0) {
					$totalSize += $remaining;
				}
			}
			$consumer->close();
		}
		return $totalSize;
	}

	/**
	 * Rewind the iterator
	 *
	 * @return void
	 */
	public function rewind() {
		$this->iterators = array();
		$this->nIterators = 0;
		foreach ($this->topicRegistry->partitions($this->topic) as $broker => $nPartitions) {
			for ($partition = 0; $partition < $nPartitions; ++$partition) {
				list($host, $port) = explode(':', $this->brokerRegistry->address($broker));
				$offset = $this->offsetRegistry->offset($this->topic, $broker, $partition);
				$this->iterators[] = (object) array(
					'consumer'          => null,
					'host'              => $host,
					'port'              => $port,
					'broker'            => $broker,
					'partition'         => $partition,
					'offset'            => $offset,
					'uncommittedOffset' => 0,
					'messages'          => null,
				);
				++$this->nIterators;
			}
		}
		if (0 == count($this->iterators)) {
			throw new Exception\InvalidTopic('Cannot find topic ' . $this->topic);
		}
		// get a random broker/partition every time
		$this->shuffle();
	}
}
