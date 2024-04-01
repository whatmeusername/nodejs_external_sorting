import fs from 'fs';
import { StringDecoder } from 'string_decoder';
import { EventEmitter } from 'events';

// примерная реализация readline.createInterface в которой мы сможем контролировать поток строк, ставить его на паузу в любой момент без задержек
// в отличие readline.createInterface в котором паузу происходит не сразу, а с задержокой, что не подходит для реализации курсоров
class LineReader extends EventEmitter {
	constructor(filepath, readerConfig, config) {
		super();

		this.encoding = config?.encoding ?? 'utf8';
		this.filterEmpty = config?.filterEmpty ?? false;
		this.filepath = filepath;
		this.lines = [];
		this.lineFragment = '';
		this._paused = false;
		this._finished = false;
		this._ended = false;
		this.decoder = new StringDecoder(this._encoding);

		this.readStream = this.initReadStream(readerConfig);
	}

	initReadStream(readerConfig) {
		const readStream = fs.createReadStream(this.filepath, readerConfig);

		readStream.on('error', (err) => this.emit('error', err));

		readStream.on('open', () => this.emit('open'));

		readStream.on('data', (data) => {
			this.readStream.pause();
			// Декодируем полученный чаннк данных из буфера в строку
			const decodedData = data instanceof Buffer ? this.decoder.write(data) : data;

			this.lines = decodedData.split(/\r?\n/g);

			if (this.filterEmpty) this.lines.filter((l) => l.length > 0);

			this.lines[0] = this.lineFragment + this.lines[0];
			// Так как последняя строка может быть не полная из-за размера буфера, то сохраняем ее для востановление при получение нового чанка
			this.lineFragment = this.lines.pop() ?? '';

			// Создаем макрозадачу, что бы не блокировать весь поток постоянным вызом next
			setImmediate(() => this.next());
		});

		readStream.on('end', () => {
			if (this.lines.length === 0) this._finish();
			this._ended = true;
			this.emit('end');
			// Создаем макрозадачу, что бы не блокировать весь поток постоянным вызом next
			setImmediate(() => this.next());
		});

		this.readStreamd = readStream;
		return readStream;
	}

	next() {
		// Отдаем строки до тех пор, пока не закончатся чанки или поток не будет поставлен на паузу
		if (this._paused) return;
		else if (this.lines.length === 0) {
			if (this._ended) {
				// Если поток чанков закончился то отдаем последнию строку если она есть
				if (this.lineFragment) {
					this.emit('line', this.lineFragment);
					this.lineFragment = '';
				} else this._finish();
			} else this.readStream.resume();

			return;
		}

		this.emit('line', this.lines.shift());

		// Создаем макрозадачу, что бы не блокировать весь поток постоянным вызом next
		setImmediate(() => this.next());
	}

	_finish() {
		if (!this._finished) {
			this._finished = true;
			this.emit('finish');
			this.close();
		}
	}

	pause() {
		this._paused = true;
	}

	resume() {
		this._paused = false;
		// Создаем макрозадачу, что бы не блокировать весь поток постоянным вызом next
		setImmediate(() => this.next());
	}

	end() {
		if (this._ended) return;
		this._ended = true;
		this.emit('end');
	}

	close() {
		this.readStream.destroy();
		this._ended = true;
		this._lines = [];
		// Создаем макрозадачу, что бы не блокировать весь поток постоянным вызом next
		setImmediate(() => this.next());
	}
}

export { LineReader };
