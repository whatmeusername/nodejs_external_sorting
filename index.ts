import { Sizes } from './const/const';
import { ExternalSort } from './src/ExternalSort';
import { ExternalSortOrder } from './types/enum';
import { ExternalSortConfig } from './types/interface';

const config: ExternalSortConfig = {
	outputFile: './output.txt',
	entryFile: './input.txt',
	chunkDir: './chunk',
	orderBy: ExternalSortOrder.ASC,
	heatSize: Sizes.KB * 100,
	removeChunks: false,
	useLocaleOrder: true,
	chunksPointerLimit: 2,
};

new ExternalSort(config).sort();
