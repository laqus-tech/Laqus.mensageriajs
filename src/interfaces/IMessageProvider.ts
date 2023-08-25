import { ConsumeMessage, Options } from "amqplib";
import { SQS } from "aws-sdk";

export interface IMessageProvider {
    Connect(): Promise<void>;
    AddConsumer(queue: string, cb: (message: ConsumeMessage | SQS.Types.Message | null) => Promise<void>, options?: Options.Consume | null): Promise<void>;
    SendMessage(queue: string, message: Buffer, messageType?: string): Promise<void>
    CreateTopic(exchange: string, type?: string, config?: Options.AssertExchange): Promise<void | string>;
    PublishMessage(exchange: string, message: Buffer, routingKey?: string, messageType?: string): Promise<void>;
    AssignTopic(queue: string, exchange: string, routingKey: string): Promise<void>;
}

export interface IProviderOptions {
    BrokerType: BrokerType,
    ConnectionURI?: string,
    Region?: string,
    AccessKey?: string,
    SecretKey?: string,
    SessionToken?: string
}

export interface QueueMapper {
    queueURL: string,
    queueARN: string
}

enum BrokerType {
    RabbitMQ = 'RabbitMQ',
    SQS = 'SQS'
}