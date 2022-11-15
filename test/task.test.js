'use strict';

const Task = require('../src')();
const assert = require('assert');
const mongoose = require('mongoose');

describe('Task', function() {
  let cancel = null;

  before(async function() {
    await mongoose.connect('mongodb://localhost:27017/task_test');
    await Task.deleteMany({});
  });

  after(async function() {
    await mongoose.disconnect();
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

    await Task.schedule('getAnswer', new Date(), {
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

    await Task.schedule('nested.getPowerLevel', new Date(), {
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

    await Task.schedule('getAnswer', new Date(Date.now() + 100), {
      question: 'calculating...'
    });

    cancel = await Task.startPolling();
    assert.strictEqual(called, 0);

    const res = await p;
    assert.strictEqual(called, 1);
    assert.deepEqual(res.params, { question: 'calculating...' });

    await Task._currentPoll;

    const task = await Task.findById(res.task._id);
    assert.ok(task);
    assert.equal(task.status, 'succeeded');
    assert.strictEqual(task.result, 42);
  });
});