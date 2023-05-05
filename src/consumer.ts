import { Sema } from "async-sema";
import * as CloudWatchLogs from "aws-sdk/clients/cloudwatchlogs";

export type CloudWatchLogsConsumerOptions = {
  region: string;
  group: string;
  stream: string;
  retentionInDays?: number;
};

const MAX_RECORDS = 25000;
const MAX_SIZE = 1024 * 8000; // 8000 KB
const MAX_DELAY = 5000; // 5 sec

export class CloudWatchLogsConsumer {
  private readonly cwlogs: CloudWatchLogs;
  private readonly group: string;
  private readonly stream: string;
  private readonly retentionInDays?: number;

  private readonly sema = new Sema(1, { capacity: 512 });
  private flushedAt: number = Date.now();
  private initialized = false;
  private sequenceToken?: string;

  private buffer: CloudWatchLogs.InputLogEvent[] = [];
  private bufferSize: number = 0;

  public constructor(options: CloudWatchLogsConsumerOptions) {
    this.cwlogs = new CloudWatchLogs({ region: options.region });
    this.group = options.group;
    this.stream = options.stream;
    this.retentionInDays = options.retentionInDays;
  }

  public async consume(line: string) {
    if (line.length > 0) {
      this.queue(line);

      if (this.shouldFlush) {
        await this.flush();
      }
    }
  }

  public async flush() {
    if (this.flushable) {
      const events = this.buffer;
      this.buffer = [];
      this.bufferSize = 0;

      await this.sema.acquire();

      try {
        await this.prepareStream();
        const res = await this.cwlogs.putLogEvents({
          logGroupName: this.group,
          logStreamName: this.stream,
          logEvents: events,
          sequenceToken: this.sequenceToken,
        }).promise();

        this.flushedAt = Date.now();
        this.sequenceToken = res.nextSequenceToken;
      } finally {
        this.sema.release();
      }
    }
  }

  private async prepareStream() {
    if (!this.initialized) {
      this.initialized = true;

      await this.cwlogs
        .createLogGroup({
          logGroupName: this.group,
        })
        .promise()
        .catch((e) => e.name === "ResourceAlreadyExistsException" ? Promise.resolve() : Promise.reject(e));

      if (this.retentionInDays !== undefined) {
        await this.cwlogs
          .putRetentionPolicy({
            logGroupName: this.group,
            retentionInDays: this.retentionInDays,
          })
          .promise();
      }

      await this.cwlogs
        .createLogStream({
          logGroupName: this.group,
          logStreamName: this.stream,
        })
        .promise()
        .catch((e) => e.name === "ResourceAlreadyExistsException" ? Promise.resolve() : Promise.reject(e));
    }
  }

  private get flushable() {
    return this.buffer.length > 0;
  }

  private get shouldFlush() {
    if (this.buffer.length > MAX_RECORDS) {
      return true;
    }

    if (this.bufferSize > MAX_SIZE) {
      return true;
    }

    return Date.now() - this.flushedAt > MAX_DELAY;
  }

  private queue(line: string) {
    const size = Buffer.byteLength(line) + 26;

    this.buffer.push({
      message: line,
      timestamp: Date.now(),
    });
    this.bufferSize += size;
  }
}
