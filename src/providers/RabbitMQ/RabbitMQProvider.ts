import { connect, Connection, Channel, Options, ConsumeMessage, Message } from "amqplib";
import { IMessageProvider, IProviderOptions } from "../../interfaces/IMessageProvider";
import { tryParse } from "../../utils/Utils";

export class RabbitMQProvider implements IMessageProvider {
    private _options: IProviderOptions;
    private _connection!: Connection;
    private _channel: Channel | null = null;

    constructor(options: IProviderOptions) {
        this._options = options;
    }

    public async Connect(): Promise<void> {
        if (!this._connection) this._connection = await connect(this._options.ConnectionURI!);

        this._channel = await this.LaqusChannel();
    }

    private async LaqusChannel(): Promise<Channel> {
        if (!this._channel) this._channel = await this._connection.createChannel();
        return this._channel;
    }

    private async LaqusAssertQueue(queue: string, config: Options.AssertQueue = { durable: true }): Promise<void> {
        if (!this._channel) this._channel = await this.LaqusChannel();

        this._channel.prefetch(1);
        await this._channel.assertQueue(queue, config);
    }

    public async CreateTopic(exchange: string, type: string, config: Options.AssertExchange = { durable: true }): Promise<void> {
        if (!this._channel) this._channel = await this.LaqusChannel();

        await this._channel.assertExchange(exchange, type, config);
    }

    public async AssignTopic(queue: string, exchange: string, routingKey: string = ''): Promise<void> {
        if (!this._channel) this._channel = await this.LaqusChannel();

        await this.LaqusAssertQueue(queue);
        await this._channel.bindQueue(queue, exchange, routingKey);
    }

    public async PublishMessage(exchange: string, message: Buffer, routingKey: string, messageType: string = ''): Promise<void> {
        if (!this._channel) this._channel = await this.LaqusChannel();

        if (messageType) {
            await this.CreateTopic(exchange, 'direct');
            this._channel.publish(exchange, routingKey, Buffer.from(JSON.stringify({
                messageType,
                message: tryParse(message)
            })));
            return;
        }

        await this.CreateTopic(exchange, 'direct');
        this._channel.publish(exchange, routingKey, message);
        return;
    }

    public async AddConsumer(queue: string, cb: (message: ConsumeMessage | null) => Promise<void>, options: Options.Consume): Promise<void> {
        if (!this._channel) this._channel = await this.LaqusChannel();

        this._channel.consume(queue, async (msg) => {
            await cb(msg);
            if (msg) await this.remove(msg);
        }, options);
    }

    public async SendMessage(queue: string, message: Buffer, messageType: string = ''): Promise<void> {
        if (!this._channel) this._channel = await this.LaqusChannel();

        if (messageType) {
            await this.LaqusAssertQueue(queue);
            this._channel.sendToQueue(queue, Buffer.from(JSON.stringify({
                messageType,
                message: tryParse(message)
            })));

            return;
        }

        await this.LaqusAssertQueue(queue);
        this._channel.sendToQueue(queue, message);
        return;
    }

    private async remove(message: Message) {
        if (!this._channel) this._channel = await this.LaqusChannel();

        this._channel.ack(message);
    }
}