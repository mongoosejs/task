'use strict';

const config = require('@masteringjs/eslint-config');
const { defineConfig } = require('eslint/config');

module.exports = defineConfig([
  {
    files: ['src/*.js'],
    languageOptions: {
      sourceType: 'commonjs',
      globals: {
        fetch: true,
        setTimeout: true,
        process: true,
        console: true,
        clearTimeout: true
      }
    },
    extends: [config]
  }
]);
