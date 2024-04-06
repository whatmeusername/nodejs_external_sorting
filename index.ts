import { Sizes } from './const/const';
import { ExternalSort } from './src/ExternalSort';
import { ExternalSortOrder } from './types/enum';

const config = {
	outputFile: './output.txt',
	entryFile: './input.txt',
	chunkDir: './chunk',
	orderBy: ExternalSortOrder.ASC,
	heatSize: Sizes.KB * 100,
	removeChunks: true,
	useLocaleOrder: true,
	chunkReadLimit: 10,
};

new ExternalSort(config).sort();
