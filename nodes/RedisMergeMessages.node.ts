import {
	INodeType,
	INodeTypeDescription,
	IExecuteFunctions,
} from 'n8n-workflow';

import { createClient } from 'redis';

export class RedisMergeMessages implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Redis Merge Messages',
		name: 'redisMergeMessages',
		group: ['transform'],
		version: 1,
		description: 'Push, merge, check, and delete messages in Redis by id',
		defaults: {
			name: 'Redis Merge Messages',
		},
		inputs: ['main'],
		outputs: ['main'],
		properties: [
			{
				displayName: 'Redis URL',
				name: 'redisUrl',
				type: 'string',
				default: 'redis://localhost:6379',
				placeholder: 'redis://user:pass@host:port',
			},
			{
				displayName: 'ID',
				name: 'id',
				type: 'string',
				default: '',
			},
			{
				displayName: 'Text',
				name: 'text',
				type: 'string',
				default: '',
			},
			{
				displayName: 'Reverse Join?',
				name: 'reverse',
				type: 'boolean',
				default: true,
			}
		]
	};

	async execute(this: IExecuteFunctions) {
		const items = this.getInputData();
		const results = [];

		for (let i = 0; i < items.length; i++) {
			const redisUrl = this.getNodeParameter('redisUrl', i) as string;
			const id = this.getNodeParameter('id', i) as string;
			const text = this.getNodeParameter('text', i) as string;
			const reverse = this.getNodeParameter('reverse', i) as boolean;

			const key = `id:${id}`;
			const redis = createClient({ url: redisUrl });
			await redis.connect();

			await redis.rPush(key, text);
			const allMsgs = await redis.lRange(key, 0, -1);

			// So sánh text cuối cùng có khớp không (tùy logic có thể chỉnh sửa)
			const isMatch = (allMsgs[allMsgs.length - 1] === text);

			const gomMsg = reverse ? allMsgs.slice().reverse().join(' ') : allMsgs.join(' ');

			await redis.del(key);
			await redis.quit();

			results.push({
				json: {
					id,
					gomMsg,
					isMatch,
					allMsgs
				}
			});
		}

		return this.prepareOutputData(results);
	}
}
