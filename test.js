#!/usr/bin/env node

'use strict';

const BulkApi = require('./');
const options = require('../../candoris/get-schooled-sfio-config').source;
options.object = 'Contact';
options.operation = 'query';
options.pkChunking = true;
const api = new BulkApi(options);

function delay(t) {
  return new Promise(function(resolve) {
    setTimeout(resolve, t);
  });
}

async function waitForJobToComplete() {
  for (let done = false; !done;) {
    await delay(2000);
    const jobInfo = await api.getJobInfo();
    const success = api.jobInfo.numberBatchesCompleted ===
      api.jobInfo.numberBatchesTotal &&
      api.jobInfo.numberBatchesFailed === 0 &&
      api.jobInfo.numberRecordsFailed === 0;
    if (success) {
      await api.closeJob();
      done = true;
    }
    const failure = api.jobInfo.numberBatchesFailed > 0 ||
      api.jobInfo.numberRecordsFailed > 0;
    if (failure) {
      await api.abortJob();
      throw new Error(jobInfo);
    }
  }
}

async function doQuery() {
  await api.addBatch('select Id, Name from Contact');
  await waitForJobToComplete();
  const stream = await api.getQueryResults();
  stream.pipe(process.stdout);
}

doQuery()
  .catch((err) => {
    console.error(err.stack || err);
    process.exit(3);
  });