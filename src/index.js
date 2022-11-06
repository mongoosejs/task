'use strict';

const mongoose = require('mongoose');
const taskSchema = require('./taskSchema');

module.exports = function(opts, conn) {
  if (conn == null) {
    conn = mongoose;
  }
  const name = opts?.name || 'Task';
  const collectionName = opts?.collectionName || undefined;

  return conn.model(name, taskSchema, collectionName);
};