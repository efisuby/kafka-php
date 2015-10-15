<?php
/**
 * Kafka Client
 *
 * @category  Libraries
 * @package   Kafka
 * @author    Lorenzo Alberton <l.alberton@quipo.it>
 * @copyright 2012 Lorenzo Alberton
 * @license   http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @version   $Revision: $
 * @link      http://sna-projects.com/kafka/
 */

/**
 * Simple Kafka Consumer
 *
 * @category Libraries
 * @package  Kafka
 * @author   Lorenzo Alberton <l.alberton@quipo.it>
 * @license  http://www.apache.org/licenses/LICENSE-2.0 Apache License, Version 2.0
 * @link     http://sna-projects.com/kafka/
 */
namespace Kafka;

use Kafka\Exception\Socket\EOF;

class SimpleConsumer
{
	/**
	 * Latest offset available
	 *
	 * @const int
	 */
	const OFFSET_LAST = -1;

	/**
	 * Smallest offset available
	 *
	 * @const int
	 */
	const OFFSET_FIRST = -2;

	/**
	 * @var string
	 */
	protected $host = 'localhost';

	/**
	 * @var integer
	 */
	protected $port = 9092;

	/**
	 * @var Socket
	 */
	protected $socket = null;

	/**
	 * Send timeout in seconds.
	 *
	 * Combined with sendTimeoutUsec this is used for send timeouts.
	 *
	 * @var int
	 */
	private $sendTimeoutSec = 0;

	/**
	 * Send timeout in microseconds.
	 *
	 * Combined with sendTimeoutSec this is used for send timeouts.
	 *
	 * @var int
	 */
	private $sendTimeoutUsec = 100000;

	/**
	 * Recv timeout in seconds
	 *
	 * Combined with recvTimeoutUsec this is used for recv timeouts.
	 *
	 * @var int
	 */
	private $recvTimeoutSec = 0;

	/**
	 * Recv timeout in microseconds
	 *
	 * Combined with recvTimeoutSec this is used for recv timeouts.
	 *
	 * @var int
	 */
	private $recvTimeoutUsec = 250000;

	/**
	 * @var integer
	 */
	protected $socketTimeout = 10;

	/**
	 * @var integer
	 */
	protected $socketBufferSize = 1000000;

	/**
	 * @var integer
	 */
	protected $lastResponseSize = 0;

	/**
	 * Constructor
	 *
	 * @param integer $host             Kafka Hostname
	 * @param integer $port             Port
	 * @param integer $socketTimeout    Socket timeout
	 * @param integer $socketBufferSize Socket max buffer size
	 */
	public function __construct($host, $port, $socketTimeout, $socketBufferSize) {
		$this->host = $host;
		$this->port = $port;
		$this->recvTimeoutSec   = $socketTimeout;
		$this->sendTimeoutSec   = $socketTimeout;
		$this->socketBufferSize = $socketBufferSize;
	}

	/**
	 * Set recv/send socket timeouts (in seconds and microseconds)
	 *
	 * @param integer $recvTimeoutSec  Recv timeout in seconds
	 * @param integer $recvTimeoutUsec Recv timeout in microseconds
	 * @param integer $sendTimeoutSec  Send timeout in seconds
	 * @param integer $sendTimeoutUsec Send timeout in microseconds
	 *
	 * @return null
	 */
	public function setSocketTimeouts($recvTimeoutSec = 0, $recvTimeoutUsec = 250000, $sendTimeoutSec = 0, $sendTimeoutUsec = 100000) {
		$this->recvTimeoutSec  = (int) $recvTimeoutSec;
		$this->recvTimeoutUsec = (int) $recvTimeoutUsec;
		$this->sendTimeoutSec  = (int) $sendTimeoutSec;
		$this->sendTimeoutUsec = (int) $sendTimeoutUsec;
	}

	/**
	 * Connect to Kafka via socket
	 *
	 * @return void
	 */
	public function connect() {
		if (null === $this->socket) {
			$this->socket = new Socket(
				$this->host,
				$this->port,
				$this->recvTimeoutSec,
				$this->recvTimeoutUsec,
				$this->sendTimeoutSec,
				$this->sendTimeoutUsec
			);
		}
		$this->socket->connect();
	}

	/**
	 * Close the connection
	 *
	 * @return void
	 */
	public function close() {
		if (null !== $this->socket) {
			$this->socket->close();
		}
	}

	/**
	 * Send a request and fetch the response
	 *
	 * @param Request $req Request
	 *
	 * @return MessageSet $messages
	 * @throws \Kafka\Exception
	 */
	public function fetch(Request $req) {
		$this->connect();
		// send request
		$req->writeTo($this->socket);

		// get response
		$this->lastResponseSize = $this->getResponseSize();
		$responseCode           = $this->getResponseCode();
		$initialOffset          = 6;

		// validate response
		Response::validateErrorCode($responseCode);
		if ($this->lastResponseSize == 2) {
			throw new Exception\EmptyQueue();
		}

		return new MessageSet($this->socket, $initialOffset);
	}

	/**
	 * Get the last response size
	 *
	 * @return integer
	 */
	public function getLastResponseSize() {
		return $this->lastResponseSize;
	}

	/**
	 * Read the request size (4 bytes) if not read yet
	 *
	 * @return integer Size of the response buffer in bytes
	 * @throws \Kafka\Exception\Socket\EOF
	 * @throws \Kafka\Exception\Socket\Timeout
	 * @throws \Kafka\Exception when size is <=0 or >= $maxSize
	 */
	protected function getResponseSize() {
		$this->connect();
		$size = $this->socket->read(4, true);
		$unpack = unpack('N', $size);
		$size = array_shift($unpack);
		if ($size <= 0) {
			throw new Exception\OutOfRange($size . ' is not a valid response size');
		}
		return $size;
	}

	/**
	 * Read the response error code
	 *
	 * @return integer Error code
	 */
	protected function getResponseCode() {
		$this->connect();
		$data = $this->socket->read(2, true);
		$unpack = unpack('n', $data);
		return array_shift($unpack);
	}

	/**
	 *  Get a list of valid offsets (up to maxSize) before the given time.
	 *  The result is a list of offsets, in descending order.
	 *
	 * @param $topic
	 * @param $partition
	 * @param $time
	 * @param $maxNumOffsets
	 *
	 * @return array an array of offsets
	 * param $time : time in millisecs (-1 from the latest offset available, -2 from the smallest offset available)
	 *
	 */
	public function getOffsetsBefore($topic, $partition, $time, $maxNumOffsets) {
		$req = new OffsetRequest($topic, $partition, $time, $maxNumOffsets);
		try {
			$this->connect();
			// send request
			$req->writeTo($this->socket);
			//echo "\nRequest sent: ".(string)$req."\n";
		} catch (EOF $e) {
			//echo "\nReconnect in get offetset request due to socket error: " . $e->getMessage();
			// retry once
			$this->connect();
			$req->writeTo($this->socket);
		}
		//$size      = $this->getResponseSize();
		$errorCode = $this->getResponseCode();
		Response::validateErrorCode($errorCode);

		return OffsetRequest::deserializeOffsetArray($this->socket);
	}

	/**
	 * Close the socket connection if still open
	 */
	public function __destruct() {
		$this->close();
	}
}
