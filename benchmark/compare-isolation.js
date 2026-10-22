'use strict';

const { fork } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const { parentPort } = require('node:worker_threads');

const RESOURCE_REPORT_ENV = 'NODE_RUN_BENCHMARK_REPORT_RESOURCES';

if (process.env[RESOURCE_REPORT_ENV] !== undefined) {
  process.once('beforeExit', () => {
    const resourceUsage = process.resourceUsage();
    const report = {
      maxRSS: resourceUsage.maxRSS,
      role: process.env.NODE_RUN_BENCHMARK_FN === undefined ?
        'coordinator' : 'isolate',
      systemCPUTime: resourceUsage.systemCPUTime,
      type: 'resources',
      userCPUTime: resourceUsage.userCPUTime,
    };
    if (parentPort) {
      parentPort.postMessage(report);
    } else if (process.send) {
      process.send(report);
    }
  });
}

const DEFAULT_RUNS = 30;
const DEFAULT_MARGIN = 5;
const DEFAULT_MAX_CV = 10;

const tCritical95 = [
  0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306,
  2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110,
  2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
  2.052, 2.048, 2.045, 2.042,
];

function calculateStats(values) {
  const mean = values.reduce((sum, value) => sum + value, 0) /
    values.length;
  const variance = values.reduce((sum, value) =>
    sum + (value - mean) ** 2, 0) / (values.length - 1);
  const standardDeviation = Math.sqrt(variance);
  return {
    cv: standardDeviation / mean * 100,
    mean,
    standardDeviation,
  };
}

function criticalValue(degreesOfFreedom) {
  if (degreesOfFreedom <= 30) return tCritical95[degreesOfFreedom];
  if (degreesOfFreedom <= 40) return 2.021;
  if (degreesOfFreedom <= 60) return 2.000;
  if (degreesOfFreedom <= 120) return 1.980;
  return 1.960;
}

function analyzeRates(processRates, workerRates, margin, maxCV) {
  if (processRates.length !== workerRates.length || processRates.length < 2) {
    throw new RangeError('Rate samples must contain matching pairs');
  }

  const processStats = calculateStats(processRates);
  const workerStats = calculateStats(workerRates);
  const differences = processRates.map((rate, index) =>
    (workerRates[index] - rate) / rate * 100);
  const differenceStats = calculateStats(differences);
  const halfWidth = criticalValue(differences.length - 1) *
    differenceStats.standardDeviation / Math.sqrt(differences.length);
  const lower = differenceStats.mean - halfWidth;
  const upper = differenceStats.mean + halfWidth;

  let verdict;
  if (processStats.cv > maxCV || workerStats.cv > maxCV) {
    verdict = 'INCONCLUSIVE';
  } else if (lower >= -margin && upper <= margin) {
    verdict = 'PASS';
  } else if (upper < -margin || lower > margin) {
    verdict = 'DIFFERENT';
  } else {
    verdict = 'INCONCLUSIVE';
  }

  return {
    difference: differenceStats.mean,
    lower,
    processCV: processStats.cv,
    processMean: processStats.mean,
    upper,
    verdict,
    workerCV: workerStats.cv,
    workerMean: workerStats.mean,
  };
}

function usage() {
  return `Usage: node compare-isolation.js [options] <benchmark-file> [-- benchmark-args]

Options:
  --runs=N       Paired samples per isolation mode (default: ${DEFAULT_RUNS})
  --margin=N     Equivalence margin in percent (default: ${DEFAULT_MARGIN})
  --max-cv=N     Maximum coefficient of variation in percent (default: ${DEFAULT_MAX_CV})

Example:
  node compare-isolation.js --runs=30 --margin=5 \\
    buffers/buffer-equals.js -- size=16386 difflen=false n=1000000`;
}

function parseNumber(name, value, minimum, integer = false) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < minimum ||
      (integer && !Number.isInteger(parsed))) {
    const requirement = integer ?
      `an integer of at least ${minimum}` : `at least ${minimum}`;
    throw new TypeError(`${name} must be ${requirement}`);
  }
  return parsed;
}

function parseArgs(args) {
  let benchmarkPath;
  let afterSeparator = false;
  const benchmarkArgs = [];
  let runs = DEFAULT_RUNS;
  let margin = DEFAULT_MARGIN;
  let maxCV = DEFAULT_MAX_CV;

  for (const arg of args) {
    if (afterSeparator) {
      benchmarkArgs.push(arg);
    } else if (arg === '--') {
      afterSeparator = true;
    } else if (arg === '--help') {
      return { help: true };
    } else if (arg.startsWith('--runs=')) {
      runs = parseNumber('--runs', arg.slice('--runs='.length), 2, true);
    } else if (arg.startsWith('--margin=')) {
      margin = parseNumber('--margin', arg.slice('--margin='.length), 0);
    } else if (arg.startsWith('--max-cv=')) {
      maxCV = parseNumber('--max-cv', arg.slice('--max-cv='.length), 0);
    } else if (arg.startsWith('-')) {
      throw new TypeError(`Unknown option: ${arg}`);
    } else if (benchmarkPath === undefined) {
      benchmarkPath = arg;
    } else {
      throw new TypeError('Benchmark arguments must follow --');
    }
  }

  if (benchmarkPath === undefined) {
    throw new TypeError('No benchmark file specified');
  }

  return { benchmarkArgs, benchmarkPath, margin, maxCV, runs };
}

function configurationKey(configuration) {
  return JSON.stringify(Object.keys(configuration).sort().map((key) =>
    [key, configuration[key]]));
}

function configurationName(configuration) {
  return Object.keys(configuration).sort().map((key) =>
    `${key}=${JSON.stringify(configuration[key])}`).join(' ');
}

function runBenchmark(filename, benchmarkArgs, isolation) {
  return new Promise((resolve, reject) => {
    const start = process.hrtime.bigint();
    const child = fork(filename, benchmarkArgs, {
      env: {
        ...process.env,
        NODE_RUN_BENCHMARK_ISOLATION: isolation,
        [RESOURCE_REPORT_ENV]: '',
      },
      execArgv: process.execArgv.concat('--require', __filename),
      stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
    });
    const reports = new Map();
    const resourceReports = [];

    child.on('message', (data) => {
      if (data.type === 'resources') {
        if (!['coordinator', 'isolate'].includes(data.role) ||
            !Number.isFinite(data.userCPUTime) || data.userCPUTime < 0 ||
            !Number.isFinite(data.systemCPUTime) || data.systemCPUTime < 0 ||
            !Number.isFinite(data.maxRSS) || data.maxRSS <= 0) {
          child.kill();
          reject(new Error('Benchmark produced an invalid resource report'));
          return;
        }
        resourceReports.push(data);
        return;
      }
      if (data.type !== 'report') return;
      if (!data.conf || !Number.isFinite(data.rate) || data.rate <= 0) {
        child.kill();
        reject(new Error('Benchmark produced an invalid report'));
        return;
      }
      const key = configurationKey(data.conf);
      if (reports.has(key)) {
        child.kill();
        reject(new Error(`Benchmark reported configuration twice: ${key}`));
        return;
      }
      reports.set(key, { configuration: data.conf, rate: data.rate });
    });
    child.once('error', reject);
    child.once('close', (code) => {
      if (code !== 0) {
        reject(new Error(`${isolation} benchmark exited with code ${code}`));
      } else if (reports.size === 0) {
        reject(new Error(`${isolation} benchmark produced no results`));
      } else if (resourceReports.length === 0) {
        reject(new Error(`${isolation} benchmark produced no resource report`));
      } else {
        const coordinatorReports = resourceReports.filter(
          (report) => report.role === 'coordinator');
        const isolateReports = resourceReports.filter(
          (report) => report.role === 'isolate');
        if (coordinatorReports.length !== 1 ||
            isolateReports.length !== reports.size) {
          reject(new Error(`${isolation} benchmark produced an unexpected ` +
                           'number of resource reports'));
          return;
        }
        const includedReports = isolation === 'process' ?
          resourceReports : coordinatorReports;
        const coordinator = coordinatorReports[0];
        const maxIsolateRSS = Math.max(
          0, ...isolateReports.map((report) => report.maxRSS));
        resolve({
          duration: Number(process.hrtime.bigint() - start) / 1e9,
          reports,
          resourceUsage: {
            maxRSS: coordinator.maxRSS +
              (isolation === 'process' ? maxIsolateRSS : 0),
            systemCPUTime: includedReports.reduce(
              (total, report) => total + report.systemCPUTime, 0),
            userCPUTime: includedReports.reduce(
              (total, report) => total + report.userCPUTime, 0),
          },
        });
      }
    });
  });
}

function sameKeys(left, right) {
  return left.length === right.length &&
    left.every((key, index) => key === right[index]);
}

function formatSigned(value) {
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
}

function printTable(rows) {
  const table = [
    ['configuration', 'process mean', 'worker mean', 'difference',
     '95% CI', 'CV process/worker', 'result'],
    ...rows.map((row) => [
      configurationName(row.configuration),
      Math.round(row.processMean).toLocaleString('en-US'),
      Math.round(row.workerMean).toLocaleString('en-US'),
      formatSigned(row.difference),
      `[${formatSigned(row.lower)}, ${formatSigned(row.upper)}]`,
      `${row.processCV.toFixed(2)}% / ${row.workerCV.toFixed(2)}%`,
      row.verdict,
    ]),
  ];
  const widths = table[0].map((_, column) =>
    Math.max(...table.map((row) => row[column].length)));
  for (const row of table) {
    console.log(row.map((value, column) =>
      value.padEnd(widths[column])).join('  '));
  }
}

async function main(args = process.argv.slice(2)) {
  if (args.length === 0) {
    console.log(usage());
    process.exitCode = 1;
    return;
  }
  const options = parseArgs(args);
  if (options.help) {
    console.log(usage());
    return;
  }

  const filename = path.resolve(options.benchmarkPath);
  if (!fs.existsSync(filename)) {
    throw new Error(`Benchmark file not found: ${filename}`);
  }

  console.log(`Isolation comparison: ${options.benchmarkPath}`);
  console.log(`Runs: ${options.runs}  Equivalence margin: ` +
              `±${options.margin.toFixed(2)}%  Maximum CV: ` +
              `${options.maxCV.toFixed(2)}%`);
  process.stdout.write('\nCollecting paired samples');

  const samples = new Map();
  const durations = { process: 0, worker: 0 };
  const resources = {
    process: { maxRSS: 0, systemCPUTime: 0, userCPUTime: 0 },
    worker: { maxRSS: 0, systemCPUTime: 0, userCPUTime: 0 },
  };
  let expectedKeys;

  for (let iteration = 0; iteration < options.runs; iteration++) {
    const order = iteration % 2 === 0 ?
      ['process', 'worker'] : ['worker', 'process'];
    const pair = {};

    for (const isolation of order) {
      pair[isolation] = await runBenchmark(
        filename, options.benchmarkArgs, isolation);
      durations[isolation] += pair[isolation].duration;
      resources[isolation].userCPUTime +=
        pair[isolation].resourceUsage.userCPUTime;
      resources[isolation].systemCPUTime +=
        pair[isolation].resourceUsage.systemCPUTime;
      resources[isolation].maxRSS = Math.max(
        resources[isolation].maxRSS,
        pair[isolation].resourceUsage.maxRSS,
      );
    }

    const processKeys = [...pair.process.reports.keys()].sort();
    const workerKeys = [...pair.worker.reports.keys()].sort();
    if (!sameKeys(processKeys, workerKeys) ||
        (expectedKeys !== undefined &&
         (!sameKeys(processKeys, expectedKeys) ||
          !sameKeys(workerKeys, expectedKeys)))) {
      throw new Error('Benchmark configuration set changed between isolations or runs');
    }
    expectedKeys ??= processKeys;

    for (const key of expectedKeys) {
      let sample = samples.get(key);
      if (sample === undefined) {
        sample = {
          configuration: pair.process.reports.get(key).configuration,
          process: [],
          worker: [],
        };
        samples.set(key, sample);
      }
      sample.process.push(pair.process.reports.get(key).rate);
      sample.worker.push(pair.worker.reports.get(key).rate);
    }
    process.stdout.write('.');
  }
  process.stdout.write('\n\n');

  const rows = [...samples.values()].map((sample) => ({
    configuration: sample.configuration,
    ...analyzeRates(
      sample.process, sample.worker, options.margin, options.maxCV),
  }));
  printTable(rows);

  const processCPUTime = resources.process.userCPUTime +
    resources.process.systemCPUTime;
  const workerCPUTime = resources.worker.userCPUTime +
    resources.worker.systemCPUTime;
  const cpuTimeChange = (workerCPUTime - processCPUTime) /
    processCPUTime * 100;
  console.log('\nTotal CPU time:');
  console.log(`  process: ${(processCPUTime / 1e6).toFixed(2)}s ` +
              `(user ${(resources.process.userCPUTime / 1e6).toFixed(2)}s, ` +
              `system ${(resources.process.systemCPUTime / 1e6).toFixed(2)}s)`);
  console.log(`  worker:  ${(workerCPUTime / 1e6).toFixed(2)}s ` +
              `(user ${(resources.worker.userCPUTime / 1e6).toFixed(2)}s, ` +
              `system ${(resources.worker.systemCPUTime / 1e6).toFixed(2)}s)`);
  console.log(`  change:  ${formatSigned(cpuTimeChange)}`);

  const memoryChange = (resources.worker.maxRSS - resources.process.maxRSS) /
    resources.process.maxRSS * 100;
  console.log('\nPeak resident memory:');
  console.log(`  process: ${(resources.process.maxRSS / 1024).toFixed(2)} MiB`);
  console.log(`  worker:  ${(resources.worker.maxRSS / 1024).toFixed(2)} MiB`);
  console.log(`  change:  ${formatSigned(memoryChange)}`);
  console.log('  process peak combines the coordinator and largest child');

  const wallTimeChange =
    (durations.worker - durations.process) / durations.process * 100;
  console.log('\nTotal benchmark time:');
  console.log(`  process: ${durations.process.toFixed(2)}s`);
  console.log(`  worker:  ${durations.worker.toFixed(2)}s`);
  console.log(`  change:  ${formatSigned(wallTimeChange)}`);

  const different = rows.filter((row) => row.verdict === 'DIFFERENT').length;
  const inconclusive = rows.filter(
    (row) => row.verdict === 'INCONCLUSIVE').length;
  console.log();
  if (different > 0) {
    console.log(`DIFFERENT: ${different} configuration(s) exceed the ` +
                `±${options.margin.toFixed(2)}% equivalence margin.`);
    process.exitCode = 1;
  } else if (inconclusive > 0) {
    console.log(`INCONCLUSIVE: ${inconclusive} configuration(s) need more ` +
                'samples, a larger n, or a quieter system.');
    process.exitCode = 1;
  } else {
    console.log('PASS: worker and process isolation are equivalent within ' +
                `±${options.margin.toFixed(2)}%.`);
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(`Error: ${error.message}`);
    process.exitCode = 1;
  });
}

module.exports = { analyzeRates };
