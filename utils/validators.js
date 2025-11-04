const validators = {
    isValidTerm: (term) => typeof term === 'string' && term.length > 0,
    isValidStream: (stream) => typeof stream === 'string' && stream.length > 0,
    isValidDetailed: (detailed) => Array.isArray(detailed) && detailed.length > 0,
    isValidMonth: (month) => /^\d{4}-\d{2}$/.test(month),
    isValidValue: (value) => !isNaN(value)
};

module.exports = validators;

