import { Sizes } from './const/const';
import { ExternalSort } from './src/ExternalSort';
import { ExternalSortOrder } from './types/enum';

const config = {
	outputFile: './output.txt',
	entryFile: './input.txt',
	chunkDir: './chunk',
	orderBy: ExternalSortOrder.ASC,
	heatSize: Sizes.MB * 500,
	removeChunks: false,
	useLocaleOrder: true,
};

new ExternalSort(config).sort();
