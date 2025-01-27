'use strict';

const Task = require('../src')();
const assert = require('assert');
const mongoose = require('mongoose');
const sinon = require('sinon');
const time = require('../src/time');

describe('Task', function() {
  let cancel = null;
  const now = new Date('2023-06-01');

  before(async function() {
    await mongoose.connect('mongodb://localhost:27017/task_test');
    await Task.deleteMany({});
  });

  after(async function() {
    await mongoose.disconnect();
  });

  beforeEach(() => {
    sinon.stub(time, 'now').callsFake(() => new Date(now));
  });

  afterEach(() => {
    sinon.restore();
  });

  afterEach(() => {
    if (cancel != null) {
      cancel();
      cancel = null;
    }
    Task.removeAllHandlers();
  });

  it('lets you register a new task', async function() {
    let resolve;
    let reject;
    const p = new Promise((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });
    Task.registerHandler('getAnswer', (params, task) => {
      resolve({ task, params });
      return 42;
    });

    await Task.schedule('getAnswer', time.now(), {
      question: 'calculating...'
    });

    await Task.poll();

    const res = await p;
    assert.deepEqual(res.params, { question: 'calculating...' });

    const task = await Task.findById(res.task._id);
    assert.ok(task);
    assert.equal(task.status, 'succeeded');
    assert.strictEqual(task.result, 42);
  });

  it('handles repeatAfterMS', async function() {
    let resolve;
    let reject;
    const p = new Promise((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });
    Task.registerHandler('getAnswer', (params, task) => {
      resolve({ task, params });
      return 42;
    });

    const now = time.now();
    await Task.schedule('getAnswer', now, {
      question: 'calculating...'
    }, 5000);

    await Task.poll();

    const res = await p;
    assert.deepEqual(res.params, { question: 'calculating...' });

    const task = await Task.findById(res.task._id);
    assert.ok(task);
    assert.equal(task.status, 'succeeded');
    assert.strictEqual(task.result, 42);

    const futureTask = await Task.findOne({ originalTaskId: task._id, status: 'pending' });
    assert.ok(futureTask);
    assert.equal(futureTask.name, task.name);
    assert.equal(futureTask.scheduledAt.toString(), new Date(now.valueOf() + 5000));
  });

  it('handles nextScheduledAt', async function() {
    let resolve;
    let reject;
    const p = new Promise((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });
    const now = time.now();
    const nextScheduledAt = new Date(now.valueOf() + 1000000);
    Task.registerHandler('getAnswer', (params, task) => {
      resolve({ task, params });
      task.nextScheduledAt = nextScheduledAt;
      return 42;
    });

    await Task.schedule('getAnswer', now, {
      question: 'calculating...'
    });

    await Task.poll();

    const res = await p;
    assert.deepEqual(res.params, { question: 'calculating...' });

    const task = await Task.findById(res.task._id);
    assert.ok(task);
    assert.equal(task.status, 'succeeded');
    assert.strictEqual(task.result, 42);

    const futureTask = await Task.findOne({ originalTaskId: task._id, status: 'pending' });
    assert.ok(futureTask);
    assert.equal(futureTask.name, task.name);
    assert.equal(futureTask.scheduledAt.toString(), nextScheduledAt.toString());
  });

  it('lets you register a nested handler', async function() {
    let resolve;
    let reject;
    const p = new Promise((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });
    const obj = {
      nested: {
        getPowerLevel: (params, task) => {
          resolve({ task, params });
          return 9001;
        }
      }
    }
    Task.registerHandlers(obj);

    await Task.schedule('nested.getPowerLevel', time.now(), {
      question: 'what does the scouter say about his power level?'
    });

    await Task.poll();

    const res = await p;
    assert.deepEqual(res.params, { question: 'what does the scouter say about his power level?' });

    const task = await Task.findById(res.task._id);
    assert.ok(task);
    assert.equal(task.status, 'succeeded');
    assert.strictEqual(task.result, 9001);
  });

  it('handles startPolling()', async function() {
    let resolve;
    let called = 0;
    const p = new Promise((_resolve) => {
      resolve = _resolve;
    });
    Task.registerHandler('getAnswer', (params, task) => {
      ++called;
      resolve({ task, params });
      return 42;
    });

    await Task.schedule('getAnswer', new Date(time.now().valueOf() + 100), {
      question: 'calculating...'
    });

    cancel = Task.startPolling({ interval: 100, workerName: 'taco' });
    assert.strictEqual(called, 0);

    sinon.restore();
    sinon.stub(time, 'now').callsFake(() => new Date(now.valueOf() + 1000));

    const res = await p;
    assert.strictEqual(called, 1);
    assert.deepEqual(res.params, { question: 'calculating...' });

    await Task._currentPoll;

    const task = await Task.findById(res.task._id);
    assert.ok(task);
    assert.equal(task.status, 'succeeded');
    assert.equal(task.workerName, 'taco');
    assert.strictEqual(task.result, 42);
  });

  it('catches errors in task', async function() {
    let resolve;
    let reject;
    const p = new Promise((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });
    Task.registerHandler('getQuestion', async () => {
      await new Promise(resolve => setTimeout(resolve, 0));
      throw new Error('Sample error message');
    });

    let task = await Task.schedule('getQuestion', time.now().valueOf() + 100000);

    task = await Task.execute(task);

    task = await Task.findById(task._id);
    assert.ok(task);
    assert.equal(task.status, 'failed');
    assert.equal(task.error.message, 'Sample error message');
    assert.equal(task.finishedRunningAt.valueOf(), now.valueOf());
  });

  it('handles task timeouts', async function() {
    let resolve;
    let reject;
    const p = new Promise((_resolve, _reject) => {
      resolve = _resolve;
      reject = _reject;
    });
    Task.registerHandler('getQuestion', async () => {
      await new Promise(resolve => setTimeout(resolve, 10000));
    });

    let task = await Task.schedule('getQuestion', time.now().valueOf() + 100000, null, { timeoutMS: 50 });

    task = await Task.execute(task);

    task = await Task.findById(task._id);
    assert.ok(task);
    assert.equal(task.status, 'failed');
    assert.equal(task.error.message, 'Task timed out after 50 ms');
    assert.equal(task.finishedRunningAt.valueOf(), now.valueOf());
  });
});
