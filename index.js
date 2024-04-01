import { Sizes } from './const.js';
import { ExternalSort } from './ExternalSort.js';

const config = {
	outputFile: 'output.txt',
	entryFile: './input.txt',
	chunkDir: 'chunk',
	orderBy: 'asc',
	heatSize: Sizes.KB * 10,
	removeChunks: true,
	useLocaleOrder: true,
};

new ExternalSort(config).sort();
