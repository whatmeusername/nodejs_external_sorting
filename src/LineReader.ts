import { ReadStream, createReadStream } from 'fs';
import { StringDecoder } from 'string_decoder';
import { EventEmitter } from 'events';
import { LineReaderConfig } from '../types/interface';

// примерная реализация readline.createInterface в которой мы сможем контролировать поток строк, ставить его на паузу в любой момент без задержек
// в отличие readline.createInterface в котором паузу происходит не сразу, а с задержокой, что не подходит для реализации курсоров
class LineReader extends EventEmitter {
	private _encoding: BufferEncoding;
	private filterEmpty: boolean;
	private filepath: string;
	private lines: string[];
	private lineFragment: string;
	private _paused: boolean;
	private _finished: boolean;
	private _ended: boolean;
	private decoder: StringDecoder;

	private readStream: ReadStream;

	constructor(filepath: string, readerConfig: Parameters<typeof createReadStream>[1], config: LineReaderConfig) {
		super();

		this._encoding = config?.encoding ?? 'utf8';
		this.filterEmpty = config?.filterEmpty ?? false;
		this.filepath = filepath;
		this.lines = [];
		this.lineFragment = '';
		this._paused = false;
		this._finished = false;
		this._ended = false;
		this.decoder = new StringDecoder(this._encoding);

		this.readStream = this._initReadStream(readerConfig);
	}

	private _initReadStream(readerConfig: Parameters<typeof createReadStream>[1]): ReadStream {
		const readStream = createReadStream(this.filepath, readerConfig);

		readStream.on('error', (err) => this.emit('error', err));

		readStream.on('open', () => this.emit('open'));

		readStream.on('data', (data) => {
			this.readStream.pause();
			// Декодируем полученный чанк данных из буфера в строку и сразу разделяем на строки
			this.lines = (data instanceof Buffer ? this.decoder.write(data) : data).split(/\r?\n/g);
			this.lines[0] = this.lineFragment + this.lines[0];
			// Так как последняя строка может быть не полная из-за размера буфера, то сохраняем ее для востановление при получение нового чанка
			this.lineFragment = this.lines.pop() ?? '';

			if (this.filterEmpty) this.lines = this.lines.filter((l) => l.length > 0);
			setImmediate(() => this.next());
		});

		readStream.on('end', () => {
			if (this.lines.length === 0 && this.lineFragment === '') this._finish();
			this._ended = true;
			this.emit('end');
			setImmediate(() => this.next());
		});

		this.readStream = readStream;
		return readStream;
	}

	private next(): void {
		// Отдаем строки до тех пор, пока не закончатся чанки или поток не будет поставлен на паузу
		if (this._paused) return;
		else if (this.lines.length === 0) {
			if (this._ended) {
				// Если поток чанков закончился то отдаем последнию строку если она есть
				if (this.lineFragment) {
					console.log(this.lineFragment, '--------');
					this.emit('line', this.lineFragment);
					this.lineFragment = '';
				} else this._finish();
			} else this.readStream.resume();

			return;
		}

		this.emit('line', this.lines.shift());
		setImmediate(() => this.next());
	}

	private _finish(): void {
		if (!this._finished) {
			this._finished = true;
			this.emit('finish');
			this.close();
		}
	}

	public pause(): void {
		this._paused = true;
	}

	public resume(): void {
		this._paused = false;
		setImmediate(() => this.next());
	}

	public end(): void {
		if (this._ended) return;
		this._ended = true;
		this.emit('end');
	}

	public close(): void {
		this.readStream.destroy();
		this._ended = true;
		this.lines = [];
		setImmediate(() => this.next());
	}
}

export { LineReader };
