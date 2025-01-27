'use strict';

const mongoose = require('mongoose');
const time = require('./time');

const taskSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  scheduledAt: {
    type: Date,
    required: true
  },
  nextScheduledAt: {
    type: Date
  },
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
    enum: ['pending', 'in_progress', 'succeeded', 'failed', 'cancelled']
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
    filter = { $and: [{ status: 'pending' }, filter] }
  };
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
  const pollOptions = workerName ? { workerName } : null;
  let cancelled = false;
  let timeout = null;
  if (!this._cancel) {
    doPoll.call(this);
    this._cancel = () => {
      cancelled = true;
      clearTimeout(timeout)
    };
  }
  return this._cancel;

  async function doPoll() {
    if (cancelled) {
      return;
    }
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

taskSchema.statics.registerHandler = async function registerHandler(name, fn) {
  this._handlers = this._handlers || new Map();
  this._handlers.set(name, fn);
  return this;
};

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

  const additionalParams = workerName ? { workerName } : {};

  while (true) {
    let tasksInProgress = [];
    for (let i = 0; i < parallel; ++i) {
      const now = time.now();
      const task = await this.findOneAndUpdate(
        { status: 'pending', scheduledAt: { $lte: now } },
        { status: 'in_progress', startedRunningAt: now, ...additionalParams },
        { new: false }
      );

      if (task == null || task.status !== 'pending') {
        break;
      }

      task.status = 'in_progress';

      tasksInProgress.push(this.execute(task));
    }

    if (tasksInProgress.length === 0) {
      break;
    }

    await Promise.all(tasksInProgress);
  }
};

taskSchema.statics.execute = async function(task) {
  if (!this._handlers.has(task.name)) {
    return null;
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
    task.finishedRunningAt = time.now();
    task.result = result;
    await task.save();
  } catch (error) {
    task.status = 'failed';
    task.error.message = error.message;
    task.error.stack = error.stack;
    task.finishedRunningAt = time.now();
    await task.save();
  }

  if (task.nextScheduledAt != null) {
    await this.create({
      name: task.name,
      scheduledAt: new Date(task.nextScheduledAt),
      repeatAfterMS: task.repeatAfterMS,
      params: task.params,
      previousTaskId: task._id,
      originalTaskId: task.originalTaskId || task._id,
      timeoutMS: task.timeoutMS
    });
  } else if (task.repeatAfterMS != null) {
    await this.create({
      name: task.name,
      scheduledAt: new Date(task.scheduledAt.valueOf() + task.repeatAfterMS),
      repeatAfterMS: task.repeatAfterMS,
      params: task.params,
      previousTaskId: task._id,
      originalTaskId: task.originalTaskId || task._id,
      timeoutMS: task.timeoutMS
    });
  }

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
    ...options
  });
};

module.exports = taskSchema;
