import { Sizes } from './const';
import { ExternalSort } from './ExternalSort';
import { ExternalSortOrder } from './interface';

const config = {
	outputFile: 'output.txt',
	entryFile: './input.txt',
	chunkDir: 'chunk',
	orderBy: ExternalSortOrder.ASC,
	heatSize: Sizes.MB * 500,
	removeChunks: false,
	useLocaleOrder: true,
};

new ExternalSort(config).sort();
