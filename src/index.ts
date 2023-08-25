import { ConsumeMessage, Options } from "amqplib";
import { IMessageProvider, IProviderOptions } from "./interfaces/IMessageProvider";
import { RabbitMQProvider } from "./providers/RabbitMQ/RabbitMQProvider";
import { SQSProvider } from "./providers/SQS/SQSProvider";
import { SQS } from "aws-sdk";

export class MessageBrokerFactory {
    private provider: IMessageProvider;

    constructor(options: IProviderOptions) {
        switch (options.BrokerType) {
            case 'RabbitMQ':
                if (!options.ConnectionURI)
                    throw new Error('ConnectionURI is required for RabbitMQ');

                this.provider = new RabbitMQProvider(options);
                break;
            case 'SQS':
                if (!options.AccessKey || !options.SecretKey || !options.Region)
                    throw new Error('Region, AccessKey, and SecretKey are required for SQS');

                this.provider = new SQSProvider(options);
                break;
            default:
                throw new Error('Invalid BrokerType, try [SQS, RabbitMQ]');
        }
    }

    public async AddConsumer(queue: string, cb: (message: ConsumeMessage | SQS.Message | null) => Promise<void>, options: Options.Consume): Promise<void> {
        await this.provider.AddConsumer(queue, cb, options);
    }

    public async Send(queue: string, message: Buffer, messageType: string): Promise<void> {
        await this.provider.SendMessage(queue, message, messageType);
    }

    public async CreateTopic(exchange: string, type: string, config: Options.AssertExchange): Promise<void> {
        await this.provider.CreateTopic(exchange, type, config);
    }

    public async PublishMessage(exchange: string, message: Buffer, routingKey: string, messageType: string): Promise<void> {
        await this.provider.PublishMessage(exchange, message, routingKey, messageType);
    }

    public async AssignTopic(queue: string, exchange: string, routingKey: string): Promise<void> {
        await this.provider.AssignTopic(queue, exchange, routingKey);
    }

    public async Connect(): Promise<void> {
        await this.provider.Connect();
    }
}