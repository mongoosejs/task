'use strict';

const Task = require('../src')();
const assert = require('assert');
const mongoose = require('mongoose');

describe('Task', function() {
  let cancel;

  before(async function() {
    await mongoose.connect('mongodb://localhost:27017/task_test');
    await Task.deleteMany({});
  });

  after(async function() {
    await mongoose.disconnect();
  });

  afterEach(() => cancel());
  
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

    await Task.schedule('getAnswer', new Date(Date.now() + 100), {
      question: 'calculating...'
    });

    cancel = Task.startPolling();

    const res = await p;
    assert.deepEqual(res.params, { question: 'calculating...' });

    await Task._currentPoll;
    const task = await Task.findById(res.task._id);
    assert.ok(task);
    assert.equal(task.status, 'succeeded');
    assert.strictEqual(task.result, 42);
  });
});