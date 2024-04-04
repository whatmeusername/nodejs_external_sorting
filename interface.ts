enum ExternalSortOrder {
	ASC = 'asc',
	DESC = 'desc',
}

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

export { ExternalSortOrder };
export type { ExternalSortConfig, ComparerFN, LineReaderConfig };
