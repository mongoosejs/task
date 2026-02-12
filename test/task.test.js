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

    cancel();
  });


  it('poll() filters by handler names', async function() {
    let called = 0;
    Task.registerHandler('handledJob', async () => {
      ++called;
      return 'ok';
    });

    await Task.schedule('unhandledJob', time.now(), { skip: true });
    const handledTask = await Task.schedule('handledJob', time.now(), { run: true });

    await Task.poll();

    const reloadedHandledTask = await Task.findById(handledTask._id);
    assert.ok(reloadedHandledTask);
    assert.equal(reloadedHandledTask.status, 'succeeded');
    assert.equal(called, 1);

    const unhandledTask = await Task.findOne({ name: 'unhandledJob' });
    assert.ok(unhandledTask);
    assert.equal(unhandledTask.status, 'pending');
    assert.strictEqual(unhandledTask.startedRunningAt, null);
    assert.strictEqual(unhandledTask.timeoutAt, null);
    assert.strictEqual(unhandledTask.workerName, null);
  });



  it('allows startPolling() to use getCurrentTime()', async function() {
    let resolve;
    const p = new Promise((_resolve) => {
      resolve = _resolve;
    });
    Task.registerHandler('getAnswer', (params, task) => {
      resolve({ task, params });
      return 42;
    });

    await Task.schedule('getAnswer', new Date(time.now().valueOf() + 1000), {
      question: 'calculating...'
    });

    const getCurrentTime = sinon.stub().callsFake(() => new Date(now.valueOf() + 2000));
    cancel = Task.startPolling({ interval: 100, getCurrentTime });

    const res = await p;
    assert.ok(getCurrentTime.called);
    assert.deepEqual(res.params, { question: 'calculating...' });

    await Task._currentPoll;

    const task = await Task.findById(res.task._id);
    assert.ok(task);
    assert.equal(task.status, 'succeeded');
    assert.strictEqual(task.result, 42);

    cancel();
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

  it('expires timed out tasks and handles repeats', async function() {
    Task.registerHandler('timedOutJob', async () => {
      // handler intentionally does nothing (we'll simulate a timeout)
    });

    // Simulate a task that was started but "timed out" previously
    const scheduledAt = time.now();
    const startedRunningAt = new Date(scheduledAt.valueOf() - 20000);
    const timeoutMS = 10000; // 10s timeout
    const timeoutAt = new Date(startedRunningAt.valueOf() + timeoutMS);

    let timedOutTask = await Task.create({
      name: 'timedOutJob',
      scheduledAt,
      startedRunningAt,
      timeoutAt,
      status: 'in_progress',
      timeoutMS,
      params: { foo: 'bar' }
    });

    // Now simulate time after timeoutAt
    sinon.restore();
    sinon.stub(time, 'now').callsFake(() =>
      // now after the timeoutAt
      new Date(timeoutAt.valueOf() + 1000)
    );

    // Directly call expireTimedOutTasks instead of polling
    await Task.expireTimedOutTasks();

    // Reload the task and check its status
    timedOutTask = await Task.findById(timedOutTask._id);
    assert.ok(timedOutTask, 'Still found timed out task');
    assert.equal(timedOutTask.status, 'timed_out');
    assert.ok(timedOutTask.finishedRunningAt.valueOf() >= timeoutAt.valueOf());

    // If repeating, should have queued repeat (not in this test)
    const repeatTask = await Task.findOne({ previousTaskId: timedOutTask._id });
    assert.ok(!repeatTask, 'No repeat should exist for non-repeating task');

    // Now try with repeatAfterMS to verify repeat scheduled
    const repeatTaskObj = await Task.create({
      name: 'timedOutJob',
      scheduledAt,
      startedRunningAt,
      timeoutAt,
      status: 'in_progress',
      repeatAfterMS: 60000,
      timeoutMS,
      params: { foo: 'baz' }
    });

    // We should advance the fake clock further to catch this one too
    sinon.restore();
    sinon.stub(time, 'now').callsFake(() =>
      new Date(timeoutAt.valueOf() + 2000)
    );

    await Task.expireTimedOutTasks();

    const afterRepeat = await Task.findById(repeatTaskObj._id);
    assert.equal(afterRepeat.status, 'timed_out');

    // The repeat should exist and be pending
    const repeated = await Task.findOne({ previousTaskId: repeatTaskObj._id, status: 'pending' });
    assert.ok(repeated, 'A repeat should be created for timed out repeating task');
    assert.equal(repeated.name, 'timedOutJob');
    assert.deepEqual(repeated.params, { foo: 'baz' });
    assert.ok(repeated.scheduledAt.valueOf() === repeatTaskObj.scheduledAt.valueOf() + 60000);
  });

  it('creates a retry task when a timed out task has retryOnTimeoutCount', async function() {
    Task.registerHandler('timeoutRetry', async () => {
      // handler intentionally does nothing (we'll simulate a timeout)
    });

    const scheduledAt = new Date(now.valueOf() - 5000);
    const startedRunningAt = new Date(now.valueOf() - 20000);
    const timeoutAt = new Date(now.valueOf() - 1000);

    let timedOutTask = await Task.create({
      name: 'timeoutRetry',
      scheduledAt,
      startedRunningAt,
      timeoutAt,
      status: 'in_progress',
      timeoutMS: 10000,
      retryOnTimeoutCount: 2,
      params: { foo: 'bar' }
    });

    await Task.expireTimedOutTasks();

    timedOutTask = await Task.findById(timedOutTask._id);
    assert.ok(timedOutTask);
    assert.equal(timedOutTask.status, 'timed_out');
    assert.equal(timedOutTask.finishedRunningAt.valueOf(), now.valueOf());

    const retryTask = await Task.findOne({
      name: 'timeoutRetry',
      status: 'pending',
      retryOnTimeoutCount: 1
    });
    assert.ok(retryTask, 'Retry task should be created');
    assert.equal(retryTask.scheduledAt.valueOf(), scheduledAt.valueOf());
    assert.strictEqual(retryTask.startedRunningAt, null);
    assert.strictEqual(retryTask.finishedRunningAt, null);
    assert.strictEqual(retryTask.workerName, null);
    assert.strictEqual(retryTask.timeoutAt, null);
    assert.deepStrictEqual(retryTask.toObject().params, { foo: 'bar' });
    assert.ok(retryTask.error.$isEmpty());
    assert.strictEqual(retryTask.result, null);
    assert.equal(
      retryTask.schedulingTimeoutAt.valueOf(),
      now.valueOf() + 10 * 60 * 1000
    );
  });

  it('handles scheduling_timed_out tasks and schedules next repeat if needed', async function() {
    Task.registerHandler('delayedJob', async () => {
      // Will not be executed due to scheduling_timed_out logic
      return 'should not be run';
    });

    // Arrange: schedule a task whose schedulingTimeoutAt is in the past
    const scheduledAt = time.now();
    const schedulingTimeoutAt = new Date(scheduledAt.valueOf() - 1000); // already "expired"
    let task = await Task.create({
      name: 'delayedJob',
      scheduledAt,
      schedulingTimeoutAt,
      status: 'pending',
      params: { foo: 'qux' }
    });

    // Should move to scheduling_timed_out when execute is called
    task = await Task.execute(task);

    assert.ok(task);
    assert.equal(task.status, 'scheduling_timed_out');
    assert.ok(task.finishedRunningAt.valueOf() >= schedulingTimeoutAt.valueOf());

    // Should NOT have side effected and not produced result
    assert.strictEqual(task.result, undefined);

    // No repeat should exist for non-repeating task
    const repeated = await Task.findOne({ previousTaskId: task._id });
    assert.ok(!repeated, 'No repeat created for non-repeating scheduling_timed_out');

    // Now try with repeatAfterMS to verify repeat scheduled
    const scheduledAt2 = time.now();
    const schedulingTimeoutAt2 = new Date(scheduledAt2.valueOf() - 2000);
    const repeatAfterMS = 60000;

    let repeatTaskInput = await Task.create({
      name: 'delayedJob',
      scheduledAt: scheduledAt2,
      schedulingTimeoutAt: schedulingTimeoutAt2,
      status: 'pending',
      params: { bar: 'baz' },
      repeatAfterMS
    });

    let repeatTask = await Task.execute(repeatTaskInput);

    assert.ok(repeatTask);
    assert.equal(repeatTask.status, 'scheduling_timed_out');

    // The repeat should exist and be pending
    const scheduledRepeat = await Task.findOne({ previousTaskId: repeatTask._id, status: 'pending' });
    assert.ok(scheduledRepeat, 'A repeat should be created for scheduling_timed_out repeating task');
    assert.equal(scheduledRepeat.name, 'delayedJob');
    assert.deepEqual(scheduledRepeat.params, { bar: 'baz' });
    assert.ok(scheduledRepeat.scheduledAt.valueOf() === repeatTask.scheduledAt.valueOf() + repeatAfterMS);

    // Also works with nextScheduledAt
    const now = time.now();
    const nextScheduledAt = new Date(now.valueOf() + 100000);
    let taskWithNext = await Task.create({
      name: 'delayedJob',
      scheduledAt: now,
      schedulingTimeoutAt: new Date(now.valueOf() - 5000),
      status: 'pending',
      params: { blah: 'blah' },
      nextScheduledAt
    });

    let resultTaskWithNext = await Task.execute(taskWithNext);
    assert.equal(resultTaskWithNext.status, 'scheduling_timed_out');

    // Next repeat should be at nextScheduledAt
    const foundNext = await Task.findOne({ previousTaskId: resultTaskWithNext._id, status: 'pending' });
    assert.ok(foundNext, 'Should make repeat with nextScheduledAt');
    assert.equal(foundNext.scheduledAt.toString(), nextScheduledAt.toString());
    assert.deepEqual(foundNext.params, { blah: 'blah' });
  });

  it('uses getCurrentTime() in execute() when checking scheduling timeouts', async function() {
    Task.registerHandler('delayedJob', async () => 'should not be run');

    const baseTime = time.now();
    const schedulingTimeoutAt = new Date(baseTime.valueOf() + 1000);
    let task = await Task.create({
      name: 'delayedJob',
      scheduledAt: baseTime,
      schedulingTimeoutAt,
      status: 'pending',
      params: { foo: 'bar' }
    });

    const getCurrentTime = sinon.stub().returns(new Date(baseTime.valueOf() + 2000));
    task = await Task.execute(task, { getCurrentTime });

    assert.ok(getCurrentTime.called);
    assert.equal(task.status, 'scheduling_timed_out');
    assert.equal(task.finishedRunningAt.valueOf(), baseTime.valueOf() + 2000);
  });
});
