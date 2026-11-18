// Helpers for the text/event-stream wire format.
//
// A message is a few "field: value" lines followed by a blank line:
//
//   id: 7
//   event: order-created
//   data: {"orderId":"ORD-1042"}
//   <blank line>
//
// JSON.stringify escapes newlines, so `data` is always a single line and a
// payload can never inject extra fields or end the message early.

function formatSseMessage({ id, event, data }) {
  let message = '';
  if (id !== undefined) message += `id: ${id}\n`;
  if (event !== undefined) message += `event: ${event}\n`;
  message += `data: ${JSON.stringify(data)}\n\n`;
  return message;
}

// Wire format for an event coming out of the broadcaster.
function formatBroadcastEvent(event) {
  return formatSseMessage({ id: event.id, event: event.type, data: event });
}

module.exports = { formatSseMessage, formatBroadcastEvent };
