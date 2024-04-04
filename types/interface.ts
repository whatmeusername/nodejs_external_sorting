import { ExternalSortOrder } from './enum';

interface ExternalSortConfig {
	outputFile: string;
	entryFile: string;
	chunkDir: string;
	orderBy?: ExternalSortOrder;
	heatSize: number;
	removeChunks?: boolean;
	useLocaleOrder?: boolean;
}

interface LineReaderConfig {
	encoding?: BufferEncoding;
	filterEmpty?: boolean;
}

type ComparerFN = (a: string, b: string) => number;

export type { ExternalSortConfig, ComparerFN, LineReaderConfig };
