import { Sizes } from "./const.js";
import { ExternalSort } from "./ExternalSort.js";

const config = {
  outputFile: "output.txt",
  entryFile: "./input.txt",
  chunkDir: "chunk",
  orderBy: "asc",
  heatSize: Sizes.MB * 500,
  removeChunks: false,
  useLocaleOrder: true,
};

new ExternalSort(config).sort();
