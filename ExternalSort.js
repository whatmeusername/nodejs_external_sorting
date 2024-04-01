import fs from 'fs';
import readline from 'readline';
import { LineReader } from './LineReader.js';

class ExternalSort {
	// Конфигурация для внешний сортировки
	constructor(config) {
		this.outputFile = config.outputFile; // пути к файлу для записи результат (будет создан в случае несуществования)
		this.entryFile = config.entryFile; // путь к исходному файлу
		this.heatSize = Math.ceil(config.heatSize * 0.9); // максимальный размер памяти который может быть выделен (по умолчанию умножаем на 0.9 для погрешности)
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

		// Распредляем память для всех опреаций (оставляем немного памяти для данных из курсоров (linesData))
		const readerHeatSize = Math.floor((this.heatSize * 0.8) / chunkFiles.length); // пропорционально разделяем память на каждый поток (курсор)
		const writerHeatSize = Math.floor(this.heatSize * 0.1);

		// Создаем поток для запись результата, даем минимальное количество памяти, так как сборщик мусора будет очищать его между работой курсоров
		const writer = fs.createWriteStream(this.outputFile, { highWaterMark: writerHeatSize });

		// Создаем функцию компаратор
		const comparer = this._getComparer();
		let FilesToFinish = chunkFiles.length;
		let linesData = [];

		const ProcessLine = () => {
			if (linesData.length !== FilesToFinish) return;
			// Получаем строку которая подходит под критерий сортировки
			linesData = linesData.sort((a, b) => comparer(a.line, b.line));
			const ld = linesData.shift();
			writer.write(`${ld.line}\n`);
			// продолжаем работу потока (курсора), строка которого была записана в результат
			ld.reader.resume();
		};

		const readerPromises = chunkFiles.map((name) => {
			return new Promise((res) => {
				// Делаем курсор для каждого потока
				const reader = new LineReader(
					`${this.chunkDir}/${name}`,
					{ highWaterMark: readerHeatSize },
					{ encoding: 'utf8', filterEmpty: true },
				);

				reader.on('line', (line) => {
					linesData.push({ line, reader });
					// ставим курсор на паузу, до тех пор пока он не будет выбран
					reader.pause();
					// Начинаем записывать строки при условии, того что каждый курсор вернул нам первую строку
					ProcessLine();
				});

				reader.once('finish', () => {
					// Поток закончил работу (прочитал все строки) избавляемся из общего количества чанков для записи
					--FilesToFinish;
					if (FilesToFinish > 0) ProcessLine();
				});

				reader.once('end', () => res(name));

				return reader;
			});
		});

		// Создаем возможность ожидать записи всех чанков посредством промизов
		return Promise.all(readerPromises);
	}

	_clearChunkDir() {
		// метод для удаления директории чанков
		if (fs.existsSync(this.chunkDir)) {
			fs.rmSync(this.chunkDir, { recursive: true, force: true });
		}
	}

	_createChunkDir() {
		if (!fs.existsSync(this.chunkDir)) {
			fs.mkdirSync(this.chunkDir);
		}
	}

	async sort() {
		// если исходного файла по данному пути не существует выбрасываем исключение
		if (!fs.existsSync(this.entryFile)) throw new Error(`Entry file: ${this.entryFile} does not exist`);
		// Чистим предыдущие чанки если они есть
		this._clearChunkDir();
		// создаем директорию для чанков если ее еще нет
		this._createChunkDir();
		// Ожидаем разделения на чанки
		await this._SplitIntoChunks();
		//Ожидаем сортировки чанков в единный файл
		await this._MergeSortedFiles();
		if (this.removeChunks) this._clearChunkDir();
	}
}

export { ExternalSort };
