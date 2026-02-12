'use strict';

const mongoose = require('mongoose');
const time = require('./time');

const taskSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  // The time at which the task was scheduled to run. The task will start running at or after this time.
  scheduledAt: {
    type: Date,
    required: true
  },
  // If the task has not started running by this time, it will be marked as `scheduling_timed_out`
  // and the next scheduled task will be created.
  schedulingTimeoutAt: {
    type: Date
  },
  // The next time this task will be scheduled to run.
  nextScheduledAt: {
    type: Date
  },
  // When this task is done, automatically schedule the next task for scheduledAt + repeatAfterMS
  repeatAfterMS: {
    type: Number
  },
  timeoutMS: {
    type: Number
  },
  cancelledAt: {
    type: Date
  },
  startedRunningAt: {
    type: Date
  },
  finishedRunningAt: {
    type: Date
  },
  retryOnTimeoutCount: {
    type: Number,
    default: 0
  },
  // If this task is still running after this time, it will be marked as `timed_out`.
  timeoutAt: {
    type: Date
  },
  previousTaskId: {
    type: mongoose.ObjectId
  },
  originalTaskId: {
    type: mongoose.ObjectId
  },
  sideEffects: [{ start: Date, end: Date, name: String, params: 'Mixed', result: 'Mixed' }],
  logs: [{ timestamp: Date, message: String, extra: Object }],
  params: Object,
  status: {
    type: String,
    default: 'pending',
    enum: [
      // Waiting to run
      'pending',
      // Currently running
      'in_progress',
      // Completed successfully
      'succeeded',
      // Error occurred while executing the task
      'failed',
      // Cancelled by user
      'cancelled',
      // Task execution timed out
      'timed_out',
      // Timed out waiting for a worker to pick up the task
      'scheduling_timed_out'
    ]
  },
  result: 'Mixed',
  error: {
    message: String,
    stack: String
  },
  workerName: {
    type: String
  }
}, { timestamps: true });

taskSchema.index({ status: 1, scheduledAt: 1 });

taskSchema.methods.log = function log(message, extra) {
  this.logs.push({ timestamp: time.now(), message, extra });
  return this.save();
};

taskSchema.statics.cancelTask = async function cancelTask(filter) {
  if (filter != null) {
    filter = { $and: [{ status: 'pending' }, filter] };
  }
  const task = await this.findOneAndUpdate(filter, { status: 'cancelled', cancelledAt: new Date() }, { returnDocument: 'after' });
  return task;
};

taskSchema.methods.sideEffect = async function sideEffect(fn, params) {
  this.sideEffects.push({ timestamp: time.now(), name: fn.name, params });
  const sideEffect = this.sideEffects[this.sideEffects.length - 1];
  await this.save();
  const result = await fn(params);

  sideEffect.end = time.now();
  sideEffect.result = result;
  await this.save();

  return result;
};

taskSchema.statics.startPolling = function startPolling(options) {
  const interval = options?.interval ?? 1000;
  const workerName = options?.workerName;
  const getCurrentTime = options?.getCurrentTime;
  const pollOptions = {
    ...(workerName ? { workerName } : {}),
    ...(getCurrentTime ? { getCurrentTime } : {})
  };
  let cancelled = false;
  let timeout = null;
  const Task = this;
  if (!this._cancel) {
    doPoll.call(this);
    this._cancel = () => {
      cancelled = true;
      clearTimeout(timeout);
      Task._cancel = null;
    };
  }
  return this._cancel;

  async function doPoll() {
    if (cancelled) {
      return;
    }

    const Task = this;

    // Expire tasks that have timed out (refactored to separate function)
    await Task.expireTimedOutTasks({ getCurrentTime });

    this._currentPoll = this.poll(pollOptions);
    await this._currentPoll.then(
      () => {
        timeout = setTimeout(() => doPoll.call(this), interval);
      },
      (err) => {
        console.log(err);
        timeout = setTimeout(() => doPoll.call(this), interval);
      }
    );
  }
};

// Refactor logic for expiring timed out tasks here
taskSchema.statics.expireTimedOutTasks = async function expireTimedOutTasks(options = {}) {
  const getCurrentTime = options.getCurrentTime;
  const now = typeof getCurrentTime === 'function' ? getCurrentTime() : time.now();
  const Task = this;
  while (true) {
    const task = await Task.findOneAndUpdate(
      {
        status: 'in_progress',
        timeoutAt: { $exists: true, $lte: now }
      },
      {
        $set: {
          status: 'timed_out',
          finishedRunningAt: now
        }
      },
      { new: true }
    );

    if (!task) {
      break;
    }

    if (task.retryOnTimeoutCount > 0) {
      // Copy task data but remove _id so MongoDB generates a new one
      const taskData = task.toObject({ virtuals: false });
      delete taskData._id;
      await Task.create({
        ...taskData,
        status: 'pending',
        retryOnTimeoutCount: task.retryOnTimeoutCount - 1,
        startedRunningAt: null,
        finishedRunningAt: null,
        workerName: null,
        error: null,
        result: null,
        timeoutAt: null,
        schedulingTimeoutAt: now.valueOf() + 10 * 60 * 1000
      });
    } else {
      await _handleRepeatingTask(Task, task);
    }
  }
};

taskSchema.statics.registerHandler = async function registerHandler(name, fn) {
  this._handlers = this._handlers || new Map();
  this._handlers.set(name, fn);
  return this;
};

async function _handleRepeatingTask(Task, task) {
  if (task.nextScheduledAt != null) {
    const scheduledAt = new Date(task.nextScheduledAt);
    return Task.create({
      name: task.name,
      scheduledAt,
      repeatAfterMS: task.repeatAfterMS,
      params: task.params,
      previousTaskId: task._id,
      originalTaskId: task.originalTaskId || task._id,
      timeoutMS: task.timeoutMS,
      schedulingTimeoutAt: scheduledAt.valueOf() + 10 * 60 * 1000
    });
  } else if (task.repeatAfterMS != null) {
    const scheduledAt = new Date(task.scheduledAt.valueOf() + task.repeatAfterMS);
    return Task.create({
      name: task.name,
      scheduledAt,
      repeatAfterMS: task.repeatAfterMS,
      params: task.params,
      previousTaskId: task._id,
      originalTaskId: task.originalTaskId || task._id,
      timeoutMS: task.timeoutMS,
      schedulingTimeoutAt: scheduledAt.valueOf() + 10 * 60 * 1000
    });
  }
}

taskSchema.statics.registerHandlers = async function registerHandlers(obj, prefix) {
  this._handlers = this._handlers || new Map();
  for (const key of Object.keys(obj)) {
    const fullPath = prefix ? `${prefix}.${key}` : key;
    if (typeof obj[key] === 'function') {
      this._handlers.set(fullPath, obj[key]);
    } else if (typeof obj[key] === 'object' && obj[key] != null) {
      this.registerHandlers(obj[key], fullPath);
    }
  }
  return this;
};

taskSchema.statics.removeAllHandlers = function removeAllHandlers() {
  this._handlers = null;
  return this;
};

taskSchema.statics.poll = async function poll(opts) {
  const parallel = (opts && opts.parallel) || 1;
  const workerName = opts?.workerName;
  const getCurrentTime = opts?.getCurrentTime;
  const handlers = this._handlers || new Map();
  const registeredHandlerNames = Array.from(handlers.keys());

  const additionalParams = workerName ? { workerName } : {};

  while (true) {
    const tasksInProgress = [];
    for (let i = 0; i < parallel; ++i) {
      const now = typeof getCurrentTime === 'function' ? getCurrentTime() : time.now();
      const filter = {
        status: 'pending',
        scheduledAt: { $lte: now },
        name: { $in: registeredHandlerNames }
      };
      const task = await this.findOneAndUpdate(
        filter,
        {
          status: 'in_progress',
          startedRunningAt: now,
          timeoutAt: new Date(now.valueOf() + 10 * 60 * 1000), // 10 minutes from startedRunningAt
          ...additionalParams
        },
        { new: false }
      );

      if (task == null || task.status !== 'pending') {
        break;
      }

      tasksInProgress.push(this.execute(task, { getCurrentTime }));
    }

    if (tasksInProgress.length === 0) {
      break;
    }

    await Promise.all(tasksInProgress);
  }
};

taskSchema.statics.execute = async function(task, options = {}) {
  if (!this._handlers || !this._handlers.has(task.name)) {
    return null;
  }

  const getCurrentTime = options.getCurrentTime;
  const currentTime = () => (typeof getCurrentTime === 'function' ? getCurrentTime() : time.now());

  task.status = 'in_progress';
  const now = currentTime();
  task.startedRunningAt = now;

  if (task.schedulingTimeoutAt && task.schedulingTimeoutAt < now) {
    task.status = 'scheduling_timed_out';
    task.finishedRunningAt = now;
    await task.save();
    await _handleRepeatingTask(this, task);
    return task;
  }

  try {
    let result = null;
    if (typeof task.timeoutMS === 'number') {
      result = await Promise.race([
        Promise.resolve(
          this._handlers.get(task.name).call(task, task.params, task)
        ),
        new Promise((_, reject) => {
          setTimeout(() => reject(new Error(`Task timed out after ${task.timeoutMS} ms`)), task.timeoutMS);
        })
      ]);
    } else {
      result = await Promise.resolve(
        this._handlers.get(task.name).call(task, task.params, task)
      );
    }
    task.status = 'succeeded';
    task.finishedRunningAt = currentTime();
    task.result = result;
    await task.save();
  } catch (error) {
    task.status = 'failed';
    task.error.message = error.message;
    task.error.stack = error.stack;
    task.finishedRunningAt = currentTime();
    await task.save();
  }

  await _handleRepeatingTask(this, task);

  return task;
};

taskSchema.statics.schedule = async function schedule(name, scheduledAt, params, optionsOrRepeat) {
  let repeatAfterMS = null;
  let options = optionsOrRepeat;
  if (typeof optionsOrRepeat === 'number') {
    repeatAfterMS = optionsOrRepeat;
    options = {};
  }
  return this.create({
    name,
    scheduledAt,
    params,
    repeatAfterMS,
    schedulingTimeoutAt: scheduledAt.valueOf() + 10 * 60 * 1000,
    ...options
  });
};

module.exports = taskSchema;
