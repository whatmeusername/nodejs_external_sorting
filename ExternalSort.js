import fs from 'fs';
import readline from 'readline';
import { LineReader } from './LineReader.js';

class ExternalSort {
	// Конфигурация для внешний сортировки
	constructor(config) {
		this.outputFile = config.outputFile; // пути к файлу для записи результат (будет создан в случае несуществования)
		this.entryFile = config.entryFile; // путь к исходному файлу
		this.heatSize = Math.ceil(config.heatSize * 0.9); // максимальный размер памяти который может быть выделен (по умолчанию умножаем на 0.9 для резерва)
		this.chunkDir = config.chunkDir; // пути к директории где будем хранить чанки (будет создан в случае несуществования)
		this.removeChunks = config?.removeChunks ?? false; // параметр определяющий удаление директории с чанками после заверщение сортировки
		this.orderBy = config.orderBy?.toLowerCase() ?? 'asc'; // определяет тип сортировки (возрастание и убывание)
		this.useLocaleOrder = config?.useLocaleOrder ?? true; // определяет какой подход к сортировке мы использем, через localeCompare или операторы сравнения
	}

	async _WriteChunkFile(data, i, comparer, writerHeatSize) {
		writerHeatSize = writerHeatSize ?? Math.ceil(this.heatSize * 0.2);
		data = data.sort(comparer);

		return new Promise((res) => {
			const writer = fs.createWriteStream(`${this.chunkDir}/chunk_${i}.tmp`, {
				highWaterMark: writerHeatSize,
			});

			const write = () => {
				let canWrite = true;
				while (canWrite) {
					// Пишем до тех пор пока память потока не заполнится,затем даем возможность сборщику мусора очистить его
					canWrite = writer.write(`${data.shift()}\n`);
					if (data.length === 0) {
						writer.close(() => res());
						break;
					}
				}
			};

			writer.on('drain', () => {
				// Ожидаем очистку памяти и при остаках данных продолжаем запись
				if (data.length > 0) write();
				else writer.close(() => res());
			});

			write();
		});
	}

	_getComparer() {
		if (this.useLocaleOrder) {
			if (this.orderBy === 'desc') return (a, b) => b.localeCompare(a);
			return (a, b) => a.localeCompare(b);
		} else {
			if (this.orderBy === 'desc') return (a, b) => (b > a ? 1 : -1);
			return (a, b) => (a > b ? 1 : -1);
		}
	}

	async _SplitIntoChunks() {
		// Распредляем память для всех опреаций
		const readerHeatSize = Math.floor(this.heatSize * 0.2);
		const writerHeatSize = Math.floor(this.heatSize * 0.2);
		const chunkDataHeatSize = Math.floor(this.heatSize * 0.6);

		// Создаем функцию компаратор
		const comparer = this._getComparer();
		const rl = readline.createInterface({
			// Даем потоку для чтения минимальное количество памяти, так как остальная память занята чанком (массивом чанков)
			input: fs.createReadStream(this.entryFile, {
				highWaterMark: readerHeatSize,
			}),
			crlfDelay: Infinity,
		});
		let size = 0;
		let ti = 0; // индекс чанка (для названия)
		let data = [];

		for await (const line of rl) {
			size += line.length;
			if (size >= chunkDataHeatSize) {
				await this._WriteChunkFile(data, ti++, comparer, writerHeatSize);
				size = 0;
				data = [];
			}
			data.push(line);
		}

		// Сохраняем оставшийся строки в чанк
		if (data.length > 0) await this._WriteChunkFile(data, ti++);
	}

	async _MergeSortedFiles() {
		// Получаем список чанков, фильтруем, что бы избавиться от системных файлов (пример .DS_STORE)
		const chunkFiles = fs.readdirSync(this.chunkDir).filter((file) => file.startsWith('chunk'));
		// Создаем поток для запись результата, даем минимальное количество памяти, так как сборщик мусора будет очищать его между работой курсоров
		const writer = fs.createWriteStream(this.outputFile, {
			highWaterMark: Math.ceil(this.heatSize * 0.1),
		});

		// Создаем функцию компаратор
		const comparer = this._getComparer();
		let FilesToFinish = chunkFiles.length;
		let linesData = [];

		const ProcessLine = () => {
			// Получаем строку которая подходит под критерий сортировки
			linesData = linesData.sort((a, b) => comparer(a.line, b.line));
			const ld = linesData.shift();
			writer.write(`${ld.line}\n`);
			// продолжаем работу потока (курсора), строка которога была записана в результат
			ld.reader.resume();
		};

		const readerPromises = chunkFiles.map((name) => {
			return new Promise((res) => {
				// Делаем курсор для каждого потока, пропорционально разделяем память на каждый поток
				const reader = new LineReader(
					`${this.chunkDir}/${name}`,
					{ highWaterMark: Math.ceil(this.heatSize / chunkFiles.length) },
					{ encoding: 'utf8', filterEmpty: true },
				);

				reader.on('line', (line) => {
					linesData.push({ line: line, reader: reader });
					// ставим курсор на паузу, до тех пор пока он не будет выбран
					reader.pause();
					// Начинаем записывать строки при условии, того что каждый курсор вернул нам первую строку
					if (linesData.length === FilesToFinish) {
						ProcessLine();
					}
				});

				reader.on('finish', () => {
					// Поток закончил работу (прочитал все строки) избавляемся из общего количества чанков для записи
					--FilesToFinish;
					reader.once('close', () => res(name));
					if (linesData.length > 0) ProcessLine();
				});

				return reader;
			});
		});

		// Создаем возможность ожидать записи всех чанков посредством промизов
		return Promise.all(readerPromises);
	}

	_clear() {
		// метод для удаления директории чанков
		if (fs.existsSync(this.chunkDir)) {
			fs.rmSync(this.chunkDir, { recursive: true, force: true });
		}
	}

	async sort() {
		// если исходного файла по данному пути не существует выбрасываем исключение
		if (!fs.existsSync(this.entryFile)) throw new Error(`Entry file: ${this.entryFile} does not exist`);
		// Чистим предыдущие чанки если они есть
		this._clear();

		// создаем директорию для чанков если ее еще нет
		if (!fs.existsSync(this.chunkDir)) {
			fs.mkdirSync(this.chunkDir);
		}
		// Ожидаем разделения на чанки
		await this._SplitIntoChunks();
		// Ожидаем сортировки чанков в единный файл
		await this._MergeSortedFiles();
		if (this.removeChunks) this._clear();
	}
}

export { ExternalSort };
