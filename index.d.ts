import { Model, Connection } from 'mongoose';

interface Options {
  // The model name for the task model. Defaults to "Task"
  name?: string;
  // The collection name that tasks will be stored in. Defaults to "tasks"
  collectionName?: string;
}

export default function createTaskModel(
  opts?: Options,
  conn?: Connection
): Model<unknown>;
