// Preserve actual timestamps and break lines where the connected collector missed a minute.
export function historySeries(values, column) {
    const rows = values.map(value => value.split(',')).map(fields => [Number(fields[0]), fields[column] === '' ? null : Number(fields[column])]).filter(row => Number.isFinite(row[0])).sort((a,b) => a[0]-b[0]);
    const data = [];
    for (const row of rows) {
        if (data.length && row[0] - data[data.length-1][0] > 90000) data.push([data[data.length-1][0]+60000, null]);
        data.push([row[0], Number.isFinite(row[1]) ? row[1] : null]);
    }
    return data;
}
