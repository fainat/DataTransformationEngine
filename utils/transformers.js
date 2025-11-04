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

module.exports = transformers;

