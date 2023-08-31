import { IMessageProvider, IProviderOptions, QueueMapper } from "../../interfaces/IMessageProvider";
import { SNS, SQS } from "aws-sdk";
import { tryParse } from "../../utils/Utils";

export class SQSProvider implements IMessageProvider {
    private _options: IProviderOptions;
    private _queues: Map<string, QueueMapper>;
    private _topics: Map<string, string>;
    private _sqs: SQS;
    private _sns: SNS;

    constructor(options: IProviderOptions) {
        this._options = options;
        this._queues = new Map();
        this._topics = new Map();
        this._sqs = new SQS({
            region: this._options.Region,
            credentials: {
                accessKeyId: this._options.AccessKey!,
                secretAccessKey: this._options.SecretKey!,
                sessionToken: this._options.SessionToken!
            }
        });
        this._sns = new SNS({
            region: this._options.Region,
            credentials: {
                accessKeyId: this._options.AccessKey!,
                secretAccessKey: this._options.SecretKey!,
                sessionToken: this._options.SessionToken!
            }
        })
    }

    public async Connect() { }

    private async LaqusCreateQueue(queue: string): Promise<{ result: string, arnResult: string }> {
        const queueMapper = this._queues.get(queue);

        if (!queueMapper) {
            const result = await this._sqs.createQueue({ QueueName: queue }).promise();

            if (!('QueueUrl' in result) || !result.QueueUrl)
                throw new Error('QueueUrl not found');

            const arnResult = await this._sqs.getQueueAttributes({ QueueUrl: result.QueueUrl, AttributeNames: ['QueueArn'] }).promise();

            if ((!arnResult.Attributes))
                throw new Error('QueueArn not found');

            this._queues.set(queue, { queueARN: arnResult.Attributes.QueueArn, queueURL: result.QueueUrl });

            return { result: result.QueueUrl, arnResult: arnResult.Attributes.QueueArn };
        }

        return { result: queueMapper.queueURL, arnResult: queueMapper.queueARN };
    }

    async PublishMessage(exchange: string, message: Buffer, messageType: string = ''): Promise<void> {
        const topicMapper = this._topics.get(exchange);

        if (!topicMapper) {
            const returnedTopic = await this.CreateTopic(exchange);

            if (messageType) {
                await this._sns.publish({
                    TopicArn: returnedTopic,
                    Message: JSON.stringify({
                        messageType,
                        message: tryParse(message)
                    })
                }).promise();

                return;
            }

            await this._sns.publish({
                TopicArn: returnedTopic,
                Message: message.toString()
            }).promise();

            return;
        }

        await this._sns.publish({
            TopicArn: topicMapper,
            Message: message.toString()
        }).promise();
    }

    async CreateTopic(exchange: string): Promise<string> {
        const topicMapper = this._topics.get(exchange);

        if (!topicMapper) {
            const topic = await this._sns.createTopic({ Name: exchange }).promise();

            if (!topic || !topic.TopicArn)
                throw new Error('Cannot create the Topic');

            this._topics.set(exchange, topic.TopicArn);

            return topic.TopicArn;
        }

        return topicMapper;
    }

    async AssignTopic(queue: string, exchange: string): Promise<void> {
        const returnedTopic = await this.CreateTopic(exchange);
        const returnedQueue = await this.LaqusCreateQueue(queue);

        await this._sqs.setQueueAttributes({
            QueueUrl: returnedQueue.result,
            Attributes: {
                Policy: JSON.stringify({
                    Version: '2012-10-17',
                    Statement: [
                        {
                            Effect: 'Allow',
                            Principal: {
                                Service: 'sns.amazonaws.com'
                            },
                            Action: 'sqs:SendMessage',
                            Resource: returnedQueue.arnResult,
                            Condition: {
                                ArnEquals: {
                                    'aws:SourceArn': returnedTopic
                                }
                            }
                        }
                    ]
                })
            }
        }).promise()

        await this._sns.subscribe({
            Protocol: 'sqs',
            TopicArn: returnedTopic,
            Endpoint: returnedQueue.arnResult
        }).promise();
    }

    async AddConsumer(queue: string, cb: (message: SQS.Types.Message | null) => Promise<void>): Promise<void> {
        let queueMapper = this._queues.get(queue);

        if (!queueMapper || queueMapper === undefined) {
            const result = await this._sqs.createQueue({ QueueName: queue }).promise();

            if (!('QueueUrl' in result) || !result.QueueUrl)
                throw new Error('QueueUrl not found');

            const arnResult = await this._sqs.getQueueAttributes({ QueueUrl: result.QueueUrl, AttributeNames: ['QueueArn'] }).promise();

            if ((!arnResult.Attributes))
                throw new Error('QueueArn not found');

            this._queues.set(queue, {
                queueARN: arnResult.Attributes.QueueArn,
                queueURL: result.QueueUrl
            });

            queueMapper = this._queues.get(queue);
        }

        if (!queueMapper) throw new Error('Queue not found!')

        while (true) {
            const { Messages } = await this._sqs.receiveMessage({
                QueueUrl: queueMapper.queueURL,
                MaxNumberOfMessages: 1
            }).promise();

            if (Messages && Messages.length > 0) {
                await cb(Messages[0]);
                await this._sqs.deleteMessage({
                    QueueUrl: queueMapper.queueURL,
                    ReceiptHandle: Messages[0].ReceiptHandle!,
                }).promise();
            }
        }
    }

    async SendMessage(queue: string, message: Buffer, messageType: string = ''): Promise<void> {
        const queueMapper = this._queues.get(queue);

        if (!queueMapper) {
            const returnedQueue = await this.LaqusCreateQueue(queue);

            if (messageType) {
                await this._sqs.sendMessage({
                    QueueUrl: returnedQueue.result,
                    MessageBody: JSON.stringify({
                        messageType,
                        message: tryParse(message)
                    })
                }).promise();
                return;
            }

            await this._sqs.sendMessage({
                QueueUrl: returnedQueue.result,
                MessageBody: message.toString()
            }).promise();
            return;
        }

        await this._sqs.sendMessage({
            QueueUrl: queueMapper.queueURL,
            MessageBody: message.toString()
        }).promise();
    }
}