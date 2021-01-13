'use strict'

const SortedMap = require('../utils/sorted-map')

/**
 * @typedef {Object} Task
 * @property {string} topic - a name for the Task (like an id but not necessarily unique)
 * @property {number} priority - tasks are ordered by priority per peer
 * @property {number} size - the size of the task, eg the number of bytes in a block
 */

/**
 * @typedef {Object} TaskMerger
 * @property {function(task, tasksWithTopic)} hasNewInfo - given the existing tasks with the same topic, does the task add some new information? Used to decide whether to merge the task or ignore it.
 * @property {function(task, existingTask)} merge - merge the information from the given task into the existing task (with the same topic)
 */

/**
 * The task merger that is used by default.
 * Assumes that new tasks do not add any information over existing tasks,
 * and doesn't try to merge.
 */
const DefaultTaskMerger = {
  hasNewInfo () {
    return false
  },

  merge () {}
}

/**
 * Queue of requests to be processed by the engine.
 * The requests from each peer are added to the peer's queue, sorted by
 * priority.
 * Tasks are popped in priority order from the best peer - see popTasks()
 * for more details.
 */
class RequestQueue {
  /**
   * @param {TaskMerger} taskMerger
   */
  constructor (taskMerger) {
    console.log("MERGERRRRRRRRRR", taskMerger)
    this._taskMerger = taskMerger || DefaultTaskMerger
    this._byPeer = new SortedMap([], PeerTasks.compare, true)
  }

  /**
   * Push tasks onto the queue for the given peer
   *
   * @param {PeerId} peerId
   * @param {Task} tasks
   */
  pushTasks (peerId, tasks) {
    let peerTasks = this._byPeer.get(peerId.toB58String())

    if (!peerTasks) {
      peerTasks = new PeerTasks(peerId, this._taskMerger)
    }

    peerTasks.pushTasks(tasks)
    this._byPeer.set(peerId.toB58String(), peerTasks)
  }

  /**
   * Choose the peer with the least active work (or if all have the same active
   * work, the most pending tasks) and pop off the highest priority tasks until
   * the total size is at least targetMinBytes.
   * This puts the popped tasks into the "active" state, meaning they are
   * actively being processed (and cannot be modified).
   *
   * @param {number} targetMinBytes - the minimum total size of tasks to pop
   * @returns {Object}
   */
  popTasks (targetMinBytes) {
    if (this._byPeer.size === 0) {
      return { tasks: [], pendingSize: 0 }
    }

    // Get the queue of tasks for the best peer and pop off tasks up to
    // targetMinBytes
    const peerTasks = this._head()
    const { tasks, pendingSize } = peerTasks.popTasks(targetMinBytes)
    if (tasks.length === 0) {
      return { tasks, pendingSize }
    }

    const peerId = peerTasks.peerId
    if (peerTasks.isIdle()) {
      // If there are no more tasks for the peer, free up its memory
      this._byPeer.delete(peerId.toB58String())
    } else {
      // If there are still tasks remaining, update the sort order of peerTasks
      // (because it depends on the number of pending tasks)
      this._byPeer.update(0)
    }

    return {
      peerId, tasks, pendingSize
    }
  }

  _head () {
    for (const [, v] of this._byPeer) {
      return v
    }
    return undefined
  }

  /**
   * Remove the task with the given topic for the given peer.
   *
   * @param {string} topic
   * @param {PeerId} peerId
   */
  remove (topic, peerId) {
    const peerTasks = this._byPeer.get(peerId.toB58String())
    peerTasks && peerTasks.remove(topic)
  }

  /**
   * Called when the tasks for the given peer complete.
   *
   * @param {PeerId} peerId
   * @param {Task[]} tasks
   */
  tasksDone (peerId, tasks) {
    const peerTasks = this._byPeer.get(peerId.toB58String())
    if (!peerTasks) {
      return
    }

    const i = this._byPeer.indexOf(peerId.toB58String())
    for (const task of tasks) {
      peerTasks.taskDone(task)
    }

    // Marking the tasks as done takes them out of the "active" state, and the
    // sort order depends on the size of the active tasks, so we need to update
    // the order.
    this._byPeer.update(i)
  }
}

/**
 * Queue of tasks for a particular peer, sorted by priority.
 */
class PeerTasks {
  /**
   * @param {PeerId} peerId
   * @param {TaskMerger} taskMerger
   */
  constructor (peerId, taskMerger) {
    this.peerId = peerId
    this._taskMerger = taskMerger
    this._activeTotalSize = 0
    this._pending = new PendingTasks()
    this._active = new Set()
  }

  /**
   * Push tasks onto the queue.
   *
   * @param {Task[]} tasks
   */
  pushTasks (tasks) {
    for (const t of tasks) {
      this._pushTask(t)
    }
  }

  _pushTask (task) {
    // If the new task doesn't add any more information over what we
    // already have in the active queue, then we can skip the new task
    if (!this._taskHasMoreInfoThanActiveTasks(task)) {
      return
    }

    // If there is already a non-active (pending) task with this topic
    const existingTask = this._pending.get(task.topic)
    if (existingTask) {
      // If the new task has a higher priority than the old task,
      if (task.priority > existingTask.priority) {
        // Update the priority and the task's position in the queue
        this._pending.updatePriority(task.topic, task.priority)
      }

      // Merge the information from the new task into the existing task
      this._taskMerger.merge(task, existingTask)

      // A task with the topic exists, so we don't need to add
      // the new task to the queue
      return
    }

    // Push the new task onto the queue
    this._pending.add(task)
  }

  // Indicates whether the new task adds any more information over tasks that are
  // already in the active task queue
  _taskHasMoreInfoThanActiveTasks (task) {
    const tasksWithTopic = []
    for (const activeTask of this._active) {
      if (activeTask.topic === task.topic) {
        tasksWithTopic.push(activeTask)
      }
    }

    // No tasks with that topic, so the new task adds information
    if (tasksWithTopic.length === 0) {
      return true
    }

    return this._taskMerger.hasNewInfo(task, tasksWithTopic)
  }

  /**
   * Pop tasks off the queue such that the total size is at least targetMinBytes
   *
   * @param {number} targetMinBytes
   * @returns {Object}
   */
  popTasks (targetMinBytes) {
    let size = 0
    const tasks = []

    // Keep popping tasks until we get up to targetMinBytes (or one item over
    // targetMinBytes)
    const pendingTasks = this._pending.tasks()
    for (let i = 0; i < pendingTasks.length && size < targetMinBytes; i++) {
      const task = pendingTasks[i]
      tasks.push(task)
      size += task.size

      // Move tasks from pending to active
      this._pending.delete(task.topic)
      this._activeTotalSize += task.size
      this._active.add(task)
    }

    return {
      tasks, pendingSize: this._pending.totalSize
    }
  }

  /**
   * Called when a task completes.
   * Note: must be the same reference as returned from popTasks.
   *
   * @param {Task} task
   */
  taskDone (task) {
    if (this._active.has(task)) {
      this._activeTotalSize -= task.size
      this._active.delete(task)
    }
  }

  /**
   * Remove pending tasks with the given topic
   *
   * @param {string} topic
   */
  remove (topic) {
    this._pending.delete(topic)
  }

  /**
   * No work to be done, this PeerTasks object can be freed.
   *
   * @returns {boolean}
   */
  isIdle () {
    return this._pending.length === 0 && this._active.length === 0
  }

  // Compare PeerTasks
  static compare (a, b) {
    // Move peers with no pending tasks to the back of the queue
    if (a[1]._pending.length === 0) {
      return 1
    }
    if (b[1]._pending.length === 0) {
      return -1
    }

    // If the amount of active work is the same
    if (a[1]._activeTotalSize === b[1]._activeTotalSize) {
      // Choose the peer with the most pending work
      return b[1]._pending.length - a[1]._pending.length
    }

    // Choose the peer with the least amount of active work ("keep peers busy")
    return a[1]._activeTotalSize - b[1]._activeTotalSize
  }
}

/**
 * Queue of pending tasks for a particular peer, sorted by priority.
 */
class PendingTasks {
  constructor () {
    this._tasks = new SortedMap([], this._compare)
  }

  get length () {
    return this._tasks.size
  }

  // Sum of the size of all pending tasks
  get totalSize () {
    return [...this._tasks.values()].reduce((a, t) => a + t.task.size, 0)
  }

  get (topic) {
    return (this._tasks.get(topic) || {}).task
  }

  add (task) {
    this._tasks.set(task.topic, {
      created: Date.now(),
      task
    })
  }

  delete (topic) {
    this._tasks.delete(topic)
  }

  // All pending tasks, in priority order
  tasks () {
    return [...this._tasks.values()].map(i => i.task)
  }

  // Update the priority of the task with the given topic, and update the order
  updatePriority (topic, priority) {
    const obj = this._tasks.get(topic)
    if (!obj) {
      return
    }

    const i = this._tasks.indexOf(topic)
    obj.task.priority = priority
    this._tasks.update(i)
  }

  // Sort by priority desc then FIFO
  _compare (a, b) {
    if (a[1].task.priority === b[1].task.priority) {
      // FIFO
      return a[1].created - b[1].created
    }
    // Priority high -> low
    return b[1].task.priority - a[1].task.priority
  }
}

module.exports = RequestQueue
