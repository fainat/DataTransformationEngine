const { generateUUID } = require('./utils/uuid');
const validators = require('./utils/validators');
const transformers = require('./utils/transformers');

const createDataEngine = (config = {}) => {

    const engineConfig = {
        ...config,
        stream: config.stream || 'stream',
        unique: config.unique || 'unique',
    };

    const cache = new Map();

    const processRawData = (data) => {
        try {
            const cacheKey = JSON.stringify(data);
            if (cache.has(cacheKey)) {
                return cache.get(cacheKey);
            }

            const termMap = new Map();

            data.forEach(item => {
                if (!validators.isValidTerm(item.term)) {
                    console.warn(`Invalid term found:`, item);
                    return;
                }

                if (!termMap.has(item.term)) {
                    termMap.set(item.term, {
                        id: generateUUID(),
                        term: item.term,
                        info: new Map()
                    });
                }

                const termData = termMap.get(item.term);

                item.info.forEach(info => {
                    if (!validators.isValidStream(info.stream) || !validators.isValidDetailed(info.detailed)) {
                        console.warn(`Invalid info data found:`, info);
                        return;
                    }

                    const existingInfo = termData.info.get(info[engineConfig.stream]);
                    if (existingInfo) {
                        existingInfo.detailed = transformers.mergeDetailed(existingInfo.detailed, info.detailed);
                        const aggregates = transformers.calculateAggregates(existingInfo.detailed);
                        existingInfo.value = aggregates.total;
                        existingInfo.uniqueBranches = aggregates.branches.size;
                        existingInfo.monthRange = {
                            start: Math.min(...aggregates.months),
                            end: Math.max(...aggregates.months)
                        };
                    } else {
                        const aggregates = transformers.calculateAggregates(info.detailed);
                        termData.info.set(info[engineConfig.stream], {
                            ...info,
                            uniqueBranches: aggregates.branches.size,
                            monthRange: {
                                start: Math.min(...aggregates.months),
                                end: Math.max(...aggregates.months)
                            }
                        });
                    }
                });
            });

            const processed = Array.from(termMap.values()).map(item => ({
                ...item,
                info: Array.from(item.info.values()).map(info => ({
                    ...info,
                    detailed: info.detailed.sort((a, b) => b.month.localeCompare(a.month))
                }))
            }));

            cache.set(cacheKey, processed);
            return processed;
        } catch (error) {
            console.error('Error processing data:', error);
            return [];
        }
    };

    const clearCache = () => cache.clear();

    return {
        processRawData,
        clearCache,
        validators,
        transformers
    };
};

/**
 * Example usage:
 * 
 * const engine = createDataEngine({
 *     stream: 'stream', // I might want to use a different existing property to identify the stream like: stream: 'unique'
 *     unique: 'unique'
 * });
 * 
 * const data = [
 *     { term: 'term1', info: [{ stream: 'stream1', detailed: [{ month: '2025-01', branch: 'branch1', value: 100 }], unique: 'unique1' }] },
 *     { term: 'term2', info: [{ stream: 'stream2', detailed: [{ month: '2025-01', branch: 'branch2', value: 200 }], unique: 'unique2' }] }
 * ];
 * 
 */

module.exports = createDataEngine;