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