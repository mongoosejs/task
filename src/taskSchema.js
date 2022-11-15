'use strict';

const mongoose = require('mongoose');

const taskSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  scheduledAt: {
    type: Date
  },
  sideEffects: [{ start: Date, end: Date, name: String, params: 'Mixed', result: 'Mixed' }],
  logs: [{ timestamp: Date, message: String, extra: Object }],
  params: Object,
  status: {
    type: String,
    default: 'pending',
    enum: ['pending', 'in_progress', 'succeeded', 'failed']
  },
  result: 'Mixed',
  error: {
    message: String,
    stack: String
  }
}, { timestamps: true });

taskSchema.index({ status: 1, scheduledAt: 1 });

taskSchema.methods.log = function log(message, extra) {
  this.logs.push({ timestamp: new Date(), message, extra });
  return this.save();
};

taskSchema.methods.sideEffect = async function sideEffect(fn, params) {
  this.sideEffects.push({ timestamp: new Date(), name: fn.name, params });
  const sideEffect = this.sideEffects[this.sideEffects.length - 1];
  await this.save();
  const result = await fn(params);

  sideEffect.end = new Date();
  sideEffect.result = result;
  await this.save();

  return result;
};

taskSchema.statics.startPolling = function startPolling() {
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
    this._currentPoll = this.poll();
    await this._currentPoll.then(
      () => {
        timeout = setTimeout(() => doPoll.call(this), 1000);
      },
      () => {
        timeout = setTimeout(() => doPoll.call(this), 1000);
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

  while (true) {
    let tasksInProgress = [];
    for (let i = 0; i < parallel; ++i) {
      const task = await this.findOneAndUpdate(
        { status: 'pending', scheduledAt: { $lte: new Date() } },
        { status: 'in_progress' },
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

  const [result, error] = await Promise.
    resolve(this._handlers.get(task.name).call(task, task.params, task)).
    then(result => [result, null], error => [null, error]);
  if (error == null) {
    task.status = 'succeeded';
    task.result = result;
    await task.save();
  } else {
    task.status = 'failed';
    task.error.message = error.message;
    task.error.stack = error.stack;
    await task.save();
  }

  return task;
};

taskSchema.statics.schedule = async function schedule(name, scheduledAt, params) {
  return this.create({
    name,
    scheduledAt,
    params
  });
};

module.exports = taskSchema;