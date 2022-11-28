# task
Scheduled tasks with Mongoose

## Getting Started

Requires Node >= 14 and Mongoose >= 6.7.0

```javascript
const mongoose = require('mongoose');

// Task is a Mongoose model that has several helpful methods and statics
// for working with scheduled tasks.
const Task = require('@mongoosejs/task')();

// Register a handler for a named task and start polling for `sayHello` tasks
Task.registerHandler('sayHello', function sayHello() {
  console.log('Hello, World!');
});
Task.startPolling();

// Will print 'Hello, World!' after approximately 1 second
await Task.schedule('sayHello', new Date(Date.now() + 1000));
```

## Params

The 2nd param to `Task.schedule()` is an object that this framework will call the handler function with.

```javascript
Task.registerHandler('sayHello', function sayHello(params) {
  console.log(`Hello, ${params.name}!`);
});

// Will print 'Hello, Friend!' after approximately 1 second
await Task.schedule(
  'sayHello',
  new Date(Date.now() + 1000),
  { name: 'Friend' }
);
```

## Repeating Tasks

The 3rd param to `Task.schedule()` is called `repeatAfterMS`.
If `repeatAfterMS` is set, this framework will immediately reschedule the task to run after `repeatAfterMS` once the original task is done.

```javascript
Task.registerHandler('sayHello', function sayHello(params) {
  console.log(`Hello, ${params.name}!`);
});

// Will print 'Hello, Friend!' every 5 seconds, after a 1 second
// initial delay
await Task.schedule(
  'sayHello',
  new Date(Date.now() + 1000),
  { name: 'Friend' },
  5000
);
```