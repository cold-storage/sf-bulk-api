#!/usr/bin/env node

'use strict';

const axios = require('axios');
const xmlParser = require('fast-xml-parser');

/*

  Provides most of the functionality of the Salesforce Bulk API.

  https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api/asynch_api_intro.htm

  The primary focus here is on query because insert/update/delete is handled by
  the new Bulk API 2.0. Assuming there are no downsides to the 2.0 api vs the
  original, we will use that for insert/update/delete.

  Each instance of BulkApi is meant to work with a single Job. To work with a
  different job, create a new BulkApi instance.

*/
class BulkApi {

  constructor(options) {
    /*
      Required options.
    */
    if (!options.url) throw new Error('options.url required');
    if (!options.username) throw new Error('options.username required');
    if (!options.password) throw new Error('options.password required');
    if (!options.token) throw new Error('options.token required');
    if (!options.apiVersion) throw new Error('options.apiVersion required');
    this.options = options;
    /*
      The following are needed to create a job.
    */
    this.options.object = options.object || null;
    // operation options: insert, upsert, update, delete, hardDelete,
    //    query, queryAll
    this.options.operation = options.operation || null;
    this.options.externalIdFieldName = options.externalIdFieldName || null;
    // concurrencyMode options: Parallel, Serial
    this.options.concurrencyMode = options.concurrencyMode || 'Parallel';
    this.options.pkChunking = options.pkChunking || false;
    /*
      The following are set on login
    */
    this.loginResponse = null;
    this.sessionId = null;
    this.url = null;
    this.jobUrl = null;
    /*
      The following are set on job creation and when asking for job status.
    */
    this.jobResonse = null;
    this.jobId = null;
    /*
      The following are set when getting all batch info.
    */
    this.batchListResponse = null;
    this.batchIds = null;
    /*
      The following are set when adding a batch or getting a single batch info.
    */
    this.batchResponse = null;
    /*
      The following are fixed values based on options only.
    */
    this.loginUrl =
      `${this.options.url}/services/Soap/u/${this.options.apiVersion}`;
    const un = this.xmlSafe(options.username);
    const pw = this.xmlSafe(options.password) + this.xmlSafe(options.token);
    this.loginXml = `<?xml version="1.0" encoding="utf-8" ?>
<env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
  <env:Body>
    <n1:login xmlns:n1="urn:partner.soap.sforce.com">
      <n1:username>${un}</n1:username>
      <n1:password>${pw}</n1:password>
    </n1:login>
  </env:Body>
</env:Envelope>`;
    // NOTE !!! order of XML elements MATTERS.
    this.jobXml = `<?xml version="1.0" encoding="UTF-8"?>
<jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
  <operation>${this.options.operation}</operation>
  <object>${this.options.object}</object>`;
    if (this.options.externalIdFieldName) {
      this.jobXml += `
  <externalIdFieldName>${this.options.externalIdFieldName}</externalIdFieldName>`;
    }
    if (this.options.concurrencyMode) {
      this.jobXml += `
  <concurrencyMode>${this.options.concurrencyMode}</concurrencyMode>`;
    }
    this.jobXml += `
  <contentType>CSV</contentType>
</jobInfo>`;
  }

  /*
    Add a batch to a Bulk API job.

    Used both for insert/update/delete batches and for query batches.

    For query batches the data parameter will be a SOQL query.

    For insert/update/delete data will be either a CSV string or stream.

    We login for you and create a job if necessary.

    For insert, update, delete this method may be called many times.
    When all data has been added call closeJob() to let your job know
    that you are done uploading data.
  */
  async addBatch(data, jobId) {
    if (!jobId) {
      await this.createJob();
    } else {
      await this.login();
    }
    jobId = jobId || this.jobId;
    const result = await axios.post(
      `${this.jobUrl}/${jobId}/batch`,
      data, {
        headers: {
          'Content-Type': 'text/csv; charset=UTF-8',
          'X-SFDC-Session': this.sessionId
        }
      });
    this.batchResponse = result.data;
    return this.batchResponse;
  }

  /*
    Return the results of a query type job as a stream.

    We assume you already have verified that the query job was successful.

    With BULK query jobs, you don't close them till all the batches are
    Completed.

    Non BULK query jobs can be closed as soon as the batch query reqest is
    successfully posted.

    For PK Chunking queries there will be multiple batches. The first one will
    always remain in NotProcessed status. This one is ignored. But you get
    results from all the rest.

    Non PK Chunking queries will only have one batch, but the batch may have
    multiple query results. So don't assume one query result per batch.
  */
  async getQueryResults(jobId) {
    jobId = jobId || this.jobId;
    await this.getBatchInfoList(jobId);
    const streams = [];
    for (const batchId of this.batchIds) {
      // https://stackoverflow.com/a/4156156/8599429
      streams.push.apply(streams, await this.getBatchQueryResults(batchId, jobId));
    }
    return streams;
  }

  async getBatchQueryResults(batchId, jobId) {
    jobId = jobId || this.jobId;
    await this.login();
    const batchResultXml = await axios.get(
      `${this.jobUrl}/${jobId}/batch/${batchId}/result`, {
        headers: {
          'X-SFDC-Session': this.sessionId
        }
      });
    // batchResultXml gonna look like this. Usually only one result element.
    // But there could be many.
    //
    // <?xml version="1.0" encoding="UTF-8"?>
    // <result-list xmlns="http://www.force.com/2009/06/asyncapi/dataload">
    //   <result>752x00000004CJE</result>
    //   <result>752x00000004CCC</result>
    // </result-list>
    let brJson = xmlParser.parse(batchResultXml.data)['result-list'];
    if (!Array.isArray(brJson)) {
      brJson = [brJson];
    }
    const streams = [];
    for (const result of brJson) {
      console.error('resultId', result.result);
      // https://stackoverflow.com/a/4156156/8599429
      streams.push(
        await this.getBatchQueryResult(result.result, batchId, jobId));
    }
    return streams;
  }

  /*
    Gets the results of a query type job. The results of insert/update/delete
    jobs are returned by getBatchResult.

    Looks like Salesforce breaks chunks down to not be larger than 1 GB.
  */
  async getBatchQueryResult(resultId, batchId, jobId) {
    console.error(resultId, batchId, jobId);
    jobId = jobId || this.jobId;
    await this.login();
    const response = await axios.get(
      `${this.jobUrl}/${jobId}/batch/${batchId}/result/${resultId}`, {
        responseType: 'stream',
        headers: {
          'X-SFDC-Session': this.sessionId
        }
      });
    return response.data;
  }

  /*

    Return errors for insert, update, delete job as a stream.

    So this is one of the huge reasons to use the new Bulk API 2.0. This old
    Bulk API doesn't have success and error files. It just has request and
    result files. The result file is ordered same as the request file and
    looks like:

    Id                  Success   Created   Error
    0012900000DnTKGAA3  FALSE     FALSE     Some error info
    0012900000DnTKHAA3  TRUE      TRUE

    To make things even more interesting, if Salesforce stops processing a batch
    at some point, the remaining rows just aren't in the result file.

    This is low priority. Not going to implement this unless there are features
    missing in the new 2.0 API and we really need to use the old.

  */
  async getErrors(jobId) {

  }

  async getBatchErrors(batchId, jobId) {

  }

  /*
    Logs in and saves this.sessionId, this.url, and this.jobUrl for subsequent
    use.

    You should never have to call this method directly. Other methods will
    call it for you.
  */
  async login() {
    if (!this.loginResponse) {
      const reply = await axios.post(
        this.loginUrl,
        this.loginXml, {
          headers: {
            'Content-Type': 'text/xml; charset=UTF-8',
            SOAPAction: 'login'
          }
        });
      this.loginResponse = reply.data;
      this.processLoginResponse();
    }
    return this.loginResponse;
  }

  /*
    Creates a Bulk API job.

    You should not have to call this method directly. Any methods that
    need it will call it for you.

    A given instance of BulkApi will never switch jobs. So we only create
    a job if we haven't already created a job or gotten job info.
  */
  async createJob() {
    if (!this.jobResonse) {
      await this.login();
      const headers = {
        'Content-Type': 'text/xml; charset=UTF-8',
        // The batch retry thing causes issues for the results files.
        // They get messed up in strange ways, so we disable it.
        'Sforce-Disable-Batch-Retry': true,
        'X-SFDC-Session': this.sessionId
      };
      // PK chunking AKA primary key chunking allows you to get massive amounts
      // of data out quickly, but doesn't support certain things like sort, etc,
      // so you don't always want to use it.
      if (this.options.pkChunking) {
        headers['Sforce-Enable-PKChunking'] = true;
      }
      const reply = await axios.post(
        this.jobUrl,
        this.jobXml, {
          headers: headers
        });
      this.jobResonse = reply.data;
      this.processJobResponse();
    }
    return this.jobResonse;
  }

  /*
    Close the Bulk API job.

    This lets your job know you are done uploading data. Not technically
    necessary, but nice. The job will then start calculating percent
    completion.

    If you don't close your job it will remain in the list of
    "In Progress" jobs on the "Monitor Bulk Data Load Jobs" page forever,
    and that's kind of annoying.
  */
  async closeJob(jobId) {
    jobId = jobId || this.jobId;
    await this.login();
    const reply = await axios.post(
      `${this.jobUrl}/${jobId}`,
      `<?xml version="1.0" encoding="UTF-8"?>
<jobInfo
   xmlns="http://www.force.com/2009/06/asyncapi/dataload">
 <state>Closed</state>
</jobInfo>`, {
        headers: {
          'Content-Type': 'text/xml; charset=UTF-8',
          'X-SFDC-Session': this.sessionId
        }
      });
    this.jobResonse = reply.data;
    this.processJobResponse();
    return this.jobResonse;
  }

  /*
    Abort the Bulk API job.

    Not technically necessary, but it's always polite if something goes wrong
    to tell your job it has become completely useless and please get out of
    the way.

    If you don't abort your job it will remain in the list of
    "In Progress" jobs on the "Monitor Bulk Data Load Jobs" page forever,
    and that's kind of annoying.
  */
  async abortJob(jobId) {
    jobId = jobId || this.jobId;
    await this.login();
    const reply = await axios.post(
      `${this.jobUrl}/${jobId}`,
      `<?xml version="1.0" encoding="UTF-8"?>
<jobInfo
   xmlns="http://www.force.com/2009/06/asyncapi/dataload">
 <state>Aborted</state>
</jobInfo>`, {
        headers: {
          'Content-Type': 'text/xml; charset=UTF-8',
          'X-SFDC-Session': this.sessionId
        }
      });
    this.jobResonse = reply.data;
    this.processJobResponse();
    return this.jobResonse;
  }

  /*
    Get info for all the batches in a given job.

    You should never have to call this method directly. It's only needed when
    getting query results. The methods that need it will call it for you.
  */
  async getBatchInfoList(jobId) {
    jobId = jobId || this.jobId;
    await this.login();
    const reply = await axios.get(
      this.jobUrl + `/${jobId}/batch`, {
        headers: {
          'Content-Type': 'text/xml; charset=UTF-8',
          'X-SFDC-Session': this.sessionId
        }
      });
    this.batchListResponse = reply.data;
    this.processBatchListResponse();
    return this.batchListResponse;
  }

  /*
    Get job status info.

    This method will tell you if your job was successful or not.
    While not technically necessary (your job will still do whatever
    it was supposed to do), you may want to close your job on success and
    abort it on failure.

    And if your job was a query, you need to know when it's done and if it
    was successful before you can get your results.
  */
  async getJobInfo(jobId) {
    jobId = jobId || this.jobId;
    await this.login();
    const reply = await axios.get(
      this.jobUrl + `/${jobId}`, {
        headers: {
          'Content-Type': 'text/xml; charset=UTF-8',
          'X-SFDC-Session': this.sessionId
        }
      });
    this.jobResonse = reply.data;
    this.processJobResponse();
    return this.jobResonse;
  }

  xmlSafe(string) {
    return string.replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&apos;');
  }

  /*
    Here we are just saving off this.url, this.sessionId, and this.jobUrl.
  */
  processLoginResponse() {
    /* {
       "soapenv:Envelope": {
         "soapenv:Body": {
           "loginResponse": {
             "result": {
               "metadataServerUrl": "https:asdf--jdswbt.cs19.my.salesforce.com/services/Soap/m/41.0/00D29000000DQIy",
               "passwordExpired": "false",
               "sandbox": "true",
               "serverUrl": "https:asdf--jdswbt.cs19.my.salesforce.com/services/Soap/u/41.0/00D29000000DQIy",
               "sessionId": "00D29000000DQIy!AQYAQOle2qEVYPxZcsi6FXr08049di.PAhusU6tnUMNKM_wTe6dWER3pM_icW6jJg7mx8oBCQgwmyAyKCYT9BRpE1A_LL9mJ",
               "userId": "00536000002xBG9AAM",
               "userInfo": {
                 "accessibilityMode": "false",
                 "chatterExternal": "false",
                 "currencySymbol": "$",
                 "orgAttachmentFileSizeLimit": 5242880,
                 "orgDefaultCurrencyIsoCode": "USD",
                 "orgDefaultCurrencyLocale": "en_US",
                 "orgDisallowHtmlAttachments": "false",
                 "orgHasPersonAccounts": "false",
                 "organizationId": "00D29000000DQIyEAO",
                 "organizationMultiCurrency": "false",
                 "organizationName": "Wycliffe Bible Translators",
                 "profileId": "00e36000001dXfzAAE",
                 "roleId": "00E36000000SEOiEAO",
                 "sessionSecondsValid": 7200,
                 "userDefaultCurrencyIsoCode": "",
                 "userEmail": "sfadmin@candoris.com",
                 "userFullName": "Candoris Wycliffe",
                 "userId": "00536000002xBG9AAM",
                 "userLanguage": "en_US",
                 "userLocale": "en_US",
                 "userName": "asdf@candoris.com.jdswbt",
                 "userTimeZone": "America/New_York",
                 "userType": "Standard",
                 "userUiSkin": "Theme3"
               }
             }
           }
         }
       }
    } */
    const loginJson = xmlParser.parse(this.loginResponse);
    const result = loginJson['soapenv:Envelope']['soapenv:Body']['loginResponse'].result;
    this.url = result.serverUrl;
    const i = this.url.indexOf('/services/Soap/');
    this.url = this.url.substring(0, i);
    this.sessionId = result.sessionId;
    this.jobUrl = `${this.url}/services/async/${this.options.apiVersion}/job`;
  }

  /*
    Here we are just saving the job id (necessary if you just created the job)
    and the job state (necessary if you want to know if your job is still
    processing or maybe it was successful or failed).
  */
  processJobResponse() {
    /* {
      "jobInfo": {
        "id": "750290000028oigAAA",
        "operation": "insert",
        "object": "Account",
        "createdById": "00536000002xBG9AAM",
        "createdDate": "2017-12-07T11:00:29.000Z",
        "systemModstamp": "2017-12-07T11:00:29.000Z",
        "state": "Open",
        "concurrencyMode": "Parallel",
        "contentType": "CSV",
        "numberBatchesQueued": 0,
        "numberBatchesInProgress": 0,
        "numberBatchesCompleted": 0,
        "numberBatchesFailed": 0,
        "numberBatchesTotal": 0,
        "numberRecordsProcessed": 0,
        "numberRetries": 0,
        "apiVersion": 41,
        "numberRecordsFailed": 0,
        "totalProcessingTime": 0,
        "apiActiveProcessingTime": 0,
        "apexProcessingTime": 0
      }
    } */
    const jobInfo = xmlParser.parse(this.jobResonse).jobInfo;
    this.jobId = jobInfo.id;
    this.jobState = jobInfo.state;
  }

  /*
    After we get a list of the info for all the batches of our job we save
    the batch ids in this.batchIds.

    This list of batch ids is intended to be used to retrieve the results of
    your batch. So one small caveat is that we dont add any batch id if the
    batch status is NotProcessed. This is because for PK chunked queries,
    even on success, the first batch will be NotProcessed and is meant to
    be ignored when retrieving results.
  */
  processBatchListResponse() {
    /* <?xml version="1.0" encoding="UTF-8"?>
    <batchInfoList
       xmlns="http://www.force.com/2009/06/asyncapi/dataload">
     <batchInfo>
      <id>75129000001ZrYPAA0</id>
      <jobId>750290000028qdsAAA</jobId>
      <state>Completed</state>
      <createdDate>2017-12-07T17:35:33.000Z</createdDate>
      <systemModstamp>2017-12-07T17:35:35.000Z</systemModstamp>
      <numberRecordsProcessed>2</numberRecordsProcessed>
      <numberRecordsFailed>0</numberRecordsFailed>
      <totalProcessingTime>964</totalProcessingTime>
      <apiActiveProcessingTime>812</apiActiveProcessingTime>
      <apexProcessingTime>271</apexProcessingTime>
     </batchInfo>
    </batchInfoList> */
    // I like this XML parser. batchInfo will either be an object or an array
    // depending on if there are one or more batchInfos.
    this.batchIds = [];
    let batchInfo = xmlParser.parse(this.batchListResponse).batchInfoList.batchInfo;
    if (!Array.isArray(batchInfo)) {
      batchInfo = [batchInfo];
    }
    batchInfo.forEach((bi) => {
      if (bi.state !== 'NotProcessed') {
        console.log(bi.id);
        this.batchIds.push(bi.id);
      }
    });
  }
}

exports = module.exports = BulkApi;