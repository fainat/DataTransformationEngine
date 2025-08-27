const createDataEngine = () => {
    const cache = new Map();

    const validators = {
        isValidTerm: (term) => typeof term === 'string' && term.length > 0,
        isValidStream: (stream) => typeof stream === 'string' && stream.length > 0,
        isValidDetailed: (detailed) => Array.isArray(detailed) && detailed.length > 0,
        isValidMonth: (month) => /^\d{4}-\d{2}$/.test(month),
        isValidValue: (value) => !isNaN(value)
    };

    const transformers = {
        mergeDetailed: (existing, incoming) => {
            const uniqueMap = new Map();

            existing.forEach(item => {
                const key = `${item.month}-${item.branch}`;
                uniqueMap.set(key, item);
            });

            incoming.forEach(item => {
                const key = `${item.month}-${item.branch}`;
                if (!uniqueMap.has(key) || new Date(item.month) > new Date(uniqueMap.get(key).month)) {
                    uniqueMap.set(key, item);
                }
            });

            return Array.from(uniqueMap.values());
        },

        calculateAggregates: (detailed) => {
            return detailed.reduce((acc, curr) => {
                const value = Number(curr.value);
                return {
                    total: acc.total + value,
                    branches: new Set([...acc.branches, curr.branch]),
                    months: new Set([...acc.months, curr.month])
                };
            }, { total: 0, branches: new Set(), months: new Set() });
        }
    };

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
                        id: crypto.randomUUID(),
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

                    const existingInfo = termData.info.get(info.stream);
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
                        termData.info.set(info.stream, {
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

module.exports = createDataEngine;