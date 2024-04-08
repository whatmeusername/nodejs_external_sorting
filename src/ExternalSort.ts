import { createWriteStream, createReadStream, readdirSync, mkdirSync, existsSync, rmSync } from 'fs';
import { createInterface } from 'readline';
import { LineReader } from './LineReader';

import { ExternalSortConfig, ComparerFN } from '../types/interface';
import { ExternalSortOrder } from '../types/enum';

class ExternalSort {
	private outputFile: string;
	private entryFile: string;
	private heatSize: number;
	private chunkDir: string;
	private removeChunks: boolean;
	private orderBy: ExternalSortOrder;
	private useLocaleOrder: boolean;
	private chunksPointerLimit: number;
	private chunkFilename: string;

	// Конфигурация для внешний сортировки
	constructor(config: ExternalSortConfig) {
		this.outputFile = config.outputFile; // пути к файлу для записи результат (будет создан в случае несуществования)
		this.entryFile = config.entryFile; // путь к исходному файлу
		this.heatSize = Math.ceil(config.heatSize * 0.9); // максимальный размер памяти который может быть выделен (по умолчанию умножаем на 0.9 для погрешности)
		this.chunkDir = config.chunkDir; // пути к директории где будем хранить чанки (будет создан в случае несуществования)
		this.removeChunks = config?.removeChunks ?? false; // параметр определяющий удаление директории с чанками после заверщение сортировки
		this.orderBy = config?.orderBy ?? ExternalSortOrder.ASC; // определяет тип сортировки (возрастание и убывание)
		this.useLocaleOrder = config?.useLocaleOrder ?? true; // определяет какой подход к сортировке мы использем, через localeCompare или операторы сравнения
		this.chunksPointerLimit =
			config?.chunksPointerLimit && config?.chunksPointerLimit >= 2 ? config?.chunksPointerLimit : 50; // определяет максимальное количество курсоров в одно время
		this.chunkFilename = config?.chunkFilename ?? 'chunk'; // название файла чанка
	}

	private async _WriteChunkFile(
		data: string[],
		i: number,
		comparer: ComparerFN,
		writerHeatSize: number,
	): Promise<null | undefined | Error> {
		writerHeatSize = writerHeatSize ?? Math.ceil(this.heatSize * 0.2);
		data = data.sort(comparer);

		return new Promise((res) => {
			const writer = createWriteStream(`${this.chunkDir}/chunk_${i}.tmp`, {
				highWaterMark: writerHeatSize,
			});

			const write = () => {
				let canWrite = true;
				while (canWrite) {
					// Пишем до тех пор пока память потока не заполнится,затем даем возможность сборщику мусора очистить его
					canWrite = writer.write(`${data.shift()}\n`);
					if (data.length === 0) {
						writer.close(res);
						break;
					}
				}
			};

			writer.on('drain', () => {
				data.length > 0 ? write() : writer.close(res);
			});

			write();
		});
	}

	private _getComparer(): ComparerFN {
		if (this.useLocaleOrder) {
			if (this.orderBy === 'desc') return (a, b) => b.localeCompare(a);
			return (a, b) => a.localeCompare(b);
		} else {
			if (this.orderBy === 'desc') return (a, b) => (b > a ? 1 : -1);
			return (a, b) => (a > b ? 1 : -1);
		}
	}

	private async _SplitIntoChunks(): Promise<void> {
		// Распредляем память для всех операций
		const readerHeatSize = Math.floor(this.heatSize * 0.2);
		const writerHeatSize = Math.floor(this.heatSize * 0.2);
		const chunkDataHeatSize = Math.floor(this.heatSize * 0.6);

		const comparer = this._getComparer();
		const rl = createInterface({
			input: createReadStream(this.entryFile, {
				highWaterMark: readerHeatSize,
			}),
			crlfDelay: Infinity,
		});
		let size = 0;
		let ti = 0; // индекс чанка (для названия)
		let data: string[] = [];

		for await (const line of rl) {
			size += line.length;
			data.push(line);
			if (size >= chunkDataHeatSize) {
				await this._WriteChunkFile(data, ti++, comparer, writerHeatSize);
				size = 0;
				data = [];
			}
		}

		// Сохраняем оставшийся строки в чанк
		if (data.length > 0) await this._WriteChunkFile(data, ti++, comparer, writerHeatSize);
	}

	private async _StartChunkSorting(): Promise<void | string[]> {
		// Получаем список чанков, фильтруем, что бы избавиться от системных файлов (пример .DS_STORE)
		let chunkFiles = readdirSync(this.chunkDir)
			.filter((file) => file.startsWith(this.chunkFilename))
			.map((file) => `${this.chunkDir}/${file}`);

		if (this.chunksPointerLimit >= chunkFiles.length)
			return this._MergeChunksByPointers(chunkFiles, this.outputFile);

		let subPrefix = 0;

		const ProcessChunk = async (cf: string[]): Promise<string[]> => {
			let i = 0;
			let offset = 0;
			const subChunksFiles: string[] = [];
			while (offset < cf.length) {
				const filesSlice = cf.slice(offset, this.chunksPointerLimit + offset);
				if (filesSlice.length === 1) {
					subChunksFiles.push(filesSlice[0]);
					break;
				} else {
					const subChunksFile = `${this.chunkDir}/sub${subPrefix}_${this.chunkFilename}_${i++}.tmp`;
					await this._MergeChunksByPointers(filesSlice, subChunksFile);
					offset += this.chunksPointerLimit;
					subChunksFiles.push(subChunksFile);
				}
			}
			return subChunksFiles;
		};

		return new Promise(async (resolve) => {
			while (chunkFiles.length > this.chunksPointerLimit) {
				chunkFiles = await ProcessChunk(chunkFiles);
				subPrefix++;
			}
			this._MergeChunksByPointers(chunkFiles, this.outputFile).then((res) => {
				resolve(res);
			});
		});
	}

	private async _MergeChunksByPointers(chunkFiles: string[], outFileDir: string): Promise<string[]> {
		// Распредляем память для всех опреаций (оставляем немного памяти для данных из курсоров (linesData))
		const readerHeatSize = Math.floor((this.heatSize * 0.8) / chunkFiles.length); // пропорционально разделяем память на каждый поток (курсор)
		const writerHeatSize = Math.floor(this.heatSize * 0.1);

		// Создаем поток для запись результата, даем минимальное количество памяти, так как сборщик мусора будет очищать его между работой курсоров
		const writer = createWriteStream(outFileDir, { highWaterMark: writerHeatSize });

		const comparer = this._getComparer();
		let FilesToFinish = chunkFiles.length;
		let linesData: { line: string; reader: LineReader }[] = [];

		const ProcessLine = () => {
			if (linesData.length !== FilesToFinish) return;
			linesData = linesData.sort((a, b) => comparer(a.line, b.line));
			const ld = linesData.shift();
			if (ld) {
				writer.write(`${ld.line}\n`);
				ld.reader.resume(); // продолжаем работу потока (курсора), строка которого была записана в результат
			}
		};

		const readerPromises: Promise<string>[] = chunkFiles.map((file) => {
			return new Promise((res) => {
				// Делаем курсор для каждого чанка
				const reader = new LineReader(
					file,
					{ highWaterMark: readerHeatSize },
					{ encoding: 'utf8', filterEmpty: true },
				);

				reader.on('line', (line) => {
					linesData.push({ line, reader });
					// ставим курсор на паузу, до тех пор пока он не будет выбран
					reader.pause();
					ProcessLine();
				});

				reader.once('finish', () => {
					// При завершения работы потока убираем его из общего количества чанков для записи
					--FilesToFinish;
					if (FilesToFinish > 0 || linesData.length > 0) ProcessLine();
				});

				reader.once('end', () => res(file));

				return reader;
			});
		});

		// Создаем возможность ожидать записи всех чанков посредством промизов
		return Promise.all(readerPromises);
	}

	private _clearChunkDir(): void {
		if (existsSync(this.chunkDir)) {
			rmSync(this.chunkDir, { recursive: true, force: true });
		}
	}

	private _createChunkDir(): void {
		if (!existsSync(this.chunkDir)) {
			mkdirSync(this.chunkDir);
		}
	}

	public async sort(): Promise<void> {
		// если исходного файла по данному пути не существует выбрасываем исключение
		if (existsSync(this.entryFile) === false) throw new Error(`Entry file: ${this.entryFile} does not exist`);
		// Удаляем предыдущие чанки если они есть
		this._clearChunkDir();
		this._createChunkDir();
		// Ожидаем разделения на чанки
		await this._SplitIntoChunks();
		//Ожидаем сортировки чанков в единный файл
		await this._StartChunkSorting();
		if (this.removeChunks) this._clearChunkDir();
	}
}

export { ExternalSort };
