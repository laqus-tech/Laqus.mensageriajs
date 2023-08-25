export function tryParse(message: Buffer): JSON | string {
    try {
        return JSON.parse(message.toString())
    } catch (e) {
        return message.toString()
    }
}