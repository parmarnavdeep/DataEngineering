function transformOrder(line) {
  var fields = line.split(",");
  if (fields.length !== 4 ||
      !/^[1-9][0-9]*$/.test(fields[0]) ||
      !/^\d{4}-\d{2}-\d{2}$/.test(fields[1]) ||
      !/^[a-z]+$/.test(fields[2]) ||
      !/^\d+$/.test(fields[3])) {
    throw new Error("Invalid synthetic order: " + line);
  }

  return JSON.stringify({
    order_id: Number(fields[0]),
    order_date: fields[1],
    category: fields[2],
    amount_cents: Number(fields[3])
  });
}
