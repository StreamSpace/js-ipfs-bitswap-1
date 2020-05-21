'use strict'

const WantManager = require('./want-manager')
const Network = require('./network')
const DecisionEngine = require('./decision-engine')
const Notifications = require('./notifications')
const logger = require('./utils').logger
const Stats = require('./stats')
const first = require('it-first')

const defaultOptions = {
  statsEnabled: false,
  statsComputeThrottleTimeout: 1000,
  statsComputeThrottleMaxQueueSize: 1000
}
const statsKeys = [
  'blocksReceived',
  'dataReceived',
  'dupBlksReceived',
  'dupDataReceived',
  'blocksSent',
  'dataSent',
  'providesBufferLength',
  'wantListLength',
  'peerCount'
]

/**
 * JavaScript implementation of the Bitswap 'data exchange' protocol
 * used by IPFS.
 *
 * @param {Libp2p} libp2p
 * @param {Blockstore} blockstore
 * @param {Object} options
 */
class Bitswap {
  constructor (libp2p, blockstore, options) {
    this._libp2p = libp2p
    this._log = logger(this.peerInfo.id)

    this._options = Object.assign({}, defaultOptions, options)

    // stats
    this._stats = new Stats(statsKeys, {
      enabled: this._options.statsEnabled,
      computeThrottleTimeout: this._options.statsComputeThrottleTimeout,
      computeThrottleMaxQueueSize: this._options.statsComputeThrottleMaxQueueSize
    })

    // the network delivers messages
    this.network = new Network(libp2p, this, {}, this._stats)

    // local database
    this.blockstore = blockstore

    this.engine = new DecisionEngine(this.peerInfo.id, blockstore, this.network, this._stats)

    // handle message sending
    this.wm = new WantManager(this.peerInfo.id, this.network, this._stats)

    this.notifications = new Notifications(this.peerInfo.id)
  }

  get peerInfo () {
    return this._libp2p.peerInfo
  }

  // handle messages received through the network
  async _receiveMessage (peerId, incoming) {
    try {
      // Note: this allows the engine to respond to any wants in the message.
      // Processing of the blocks in the message happens below, after the
      // blocks have been added to the blockstore.
      await this.engine.messageReceived(peerId, incoming)
    } catch (err) {
      // Log instead of throwing an error so as to process as much as
      // possible of the message. Currently `messageReceived` does not
      // throw any errors, but this could change in the future.
      this._log('failed to receive message', incoming)
    }

    if (incoming.blocks.size === 0) {
      return
    }

    const blocks = Array.from(incoming.blocks.values())

    // quickly send out cancels, reduces chances of duplicate block receives
    const wanted = blocks
      .filter((b) => this.wm.wantlist.contains(b.cid))
      .map((b) => b.cid)

    this.wm.cancelWants(wanted)

    await Promise.all(blocks.map(async (b) => {
      const wasWanted = wanted.includes(b.cid)
      await this._handleReceivedBlock(peerId, b, wasWanted)
    }))
  }

  async _handleReceivedBlock (peerId, block, wasWanted) {
    this._log('received block')

    const has = await this.blockstore.has(block.cid)

    this._updateReceiveCounters(peerId.toB58String(), block, has)

    if (has || !wasWanted) {
      if (wasWanted) {
        this._sendHaveBlockNotifications(block)
      }

      return
    }

    await this.put(block)
  }

  _updateReceiveCounters (peerId, block, exists) {
    this._stats.push(peerId, 'blocksReceived', 1)
    this._stats.push(peerId, 'dataReceived', block.data.length)

    if (exists) {
      this._stats.push(peerId, 'dupBlksReceived', 1)
      this._stats.push(peerId, 'dupDataReceived', block.data.length)
    }
  }

  // handle errors on the receiving channel
  _receiveError (err) {
    this._log.error('ReceiveError: %s', err.message)
  }

  // handle new peers
  _onPeerConnected (peerId) {
    this.wm.connected(peerId)
  }

  // handle peers being disconnected
  _onPeerDisconnected (peerId) {
    this.wm.disconnected(peerId)
    this.engine.peerDisconnected(peerId)
    this._stats.disconnected(peerId)
  }

  /**
   * @returns {void}
   */
  enableStats () {
    this._stats.enable()
  }

  /**
   * @returns {void}
   */
  disableStats () {
    this._stats.disable()
  }

  /**
   * Return the current wantlist for a given `peerId`
   *
   * @param {PeerId} peerId
   * @returns {Map}
   */
  wantlistForPeer (peerId) {
    return this.engine.wantlistForPeer(peerId)
  }

  /**
   * Return ledger information for a given `peerId`
   *
   * @param {PeerId} peerId
   * @returns {Object}
   */
  ledgerForPeer (peerId) {
    return this.engine.ledgerForPeer(peerId)
  }

  /**
   * Fetch a given block by cid. If the block is in the local
   * blockstore it is returned, otherwise the block is added to the wantlist and returned once another node sends it to us.
   *
   * @param {CID} cid
   * @returns {Promise<Block>}
   */
  async get (cid) { // eslint-disable-line require-await
    return first(this.getMany([cid]))
  }

  /**
   * Fetch a a list of blocks by cid. If the blocks are in the local
   * blockstore they are returned, otherwise the blocks are added to the wantlist and returned once another node sends them to us.
   *
   * @param {AsyncIterator<CID>} cids
   * @returns {Promise<AsyncIterator<Block>>}
   */
  async * getMany (cids) {
    const fetchFromNetwork = async (cid) => {
      // add it to the want list
      this.wm.wantBlocks([cid])

      const block = await this.notifications.wantBlock(cid)

      // we've got it, remove it from the want list
      this.wm.cancelWants([cid])

      return block
    }

    let promptedNetwork = false

    const loadOrFetchFromNetwork = async (cid) => {
      try {
        // have to await here as we want to handle ERR_NOT_FOUND
        const block = await this.blockstore.get(cid)

        return block
      } catch (err) {
        if (err.code !== 'ERR_NOT_FOUND') {
          throw err
        }

        if (!promptedNetwork) {
          promptedNetwork = true

          this.network.findAndConnect(cid)
            .catch((err) => this._log.error(err))
        }

        // we don't have the block locally so fetch it from the network
        return fetchFromNetwork(cid)
      }
    }

    for (const cid of cids) {
      // depending on implementation it's possible for blocks to come in while
      // we do the async operations to get them from the blockstore leading to
      // a race condition, so register for incoming block notifications as well
      // as trying to get it from the datastore
      const block = await Promise.race([
        this.notifications.wantBlock(cid).then(block => {
          // if block is not set it means this block was unwanted while we wanted
          // it so we should be able to load it from the datastore now
          return block || loadOrFetchFromNetwork(cid)
        }),
        loadOrFetchFromNetwork(cid)
      ])

      // since we have the block we can now remove our listener
      this.notifications.unwantBlock(cid)

      yield block
    }
  }

  /**
   * Removes the given CIDs from the wantlist independent of any ref counts
   *
   * @param {Iterable<CID>} cids
   * @returns {void}
   */
  unwant (cids) {
    if (!Array.isArray(cids)) {
      cids = [cids]
    }

    this.wm.unwantBlocks(cids)
    cids.forEach((cid) => this.notifications.unwantBlock(cid))
  }

  /**
   * Removes the given keys from the want list
   *
   * @param {Iterable<CID>} cids
   * @returns {void}
   */
  cancelWants (cids) {
    if (!Array.isArray(cids)) {
      cids = [cids]
    }
    this.wm.cancelWants(cids)
  }

  /**
   * Put the given block to the underlying blockstore and
   * send it to nodes that have it in their wantlist.
   *
   * @param {Block} block
   * @returns {Promise<void>}
   */
  async put (block) { // eslint-disable-line require-await
    return first(this.putMany([block]))
  }

  /**
   * Put the given blocks to the underlying blockstore and
   * send it to nodes that have it them their wantlist.
   *
   * @param {AsyncIterable<Block>|Iterable<Block>} blocks
   * @returns {AsyncIterable<Block>}
   */
  async * putMany (blocks) { // eslint-disable-line require-await
    for await (const block of this.blockstore.putMany(blocks)) {
      this._sendHaveBlockNotifications(block)

      yield block
    }
  }

  /**
   * Sends notifications about the arrival of a block
   *
   * @param {Block} block
   */
  _sendHaveBlockNotifications (block) {
    this.notifications.hasBlock(block)
    this.engine.receivedBlocks([block])
    // Note: Don't wait for provide to finish before returning
    this.network.provide(block.cid).catch((err) => {
      this._log.error('Failed to provide: %s', err.message)
    })
  }

  /**
   * Get the current list of wants.
   *
   * @returns {Iterator<WantlistEntry>}
   */
  getWantlist () {
    return this.wm.wantlist.entries()
  }

  /**
   * Get the current list of partners.
   *
   * @returns {Iterator<PeerId>}
   */
  peers () {
    return this.engine.peers()
  }

  /**
   * Get stats about the bitswap node.
   *
   * @returns {Object}
   */
  stat () {
    return this._stats
  }

  /**
   * Start the bitswap node.
   *
   * @returns {void}
   */
  start () {
    this.wm.start()
    this.network.start()
    this.engine.start()
  }

  /**
   * Stop the bitswap node.
   *
   * @returns {void}
   */
  stop () {
    this._stats.stop()
    this.wm.stop()
    this.network.stop()
    this.engine.stop()
  }
}

module.exports = Bitswap
