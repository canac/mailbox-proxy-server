import { ulid } from "@std/ulid";

type MessageEntry = Deno.KvEntry<string>;

/**
 * Send a message over the websocket if it could be claimed. If another client claimed it first,
 * then it will not be sent.
 */
const sendMessage = async (socket: WebSocket, entry: MessageEntry) => {
  // Delete the message if it hasn't already been deleted by another client
  const result = await kv.atomic()
    .check(entry)
    .delete(entry.key)
    .commit();
  if (result.ok) {
    socket.send(entry.value);
  }
};

const authToken = Deno.env.get("AUTH_TOKEN");
if (!authToken) {
  throw new Error("$AUTH_TOKEN environment variable not set");
}

const kv = await Deno.openKv();
const sendChannel = new BroadcastChannel("message");

Deno.serve(async (req) => {
  if (req.headers.get("authorization") !== authToken) {
    return new Response("Unauthorized", { status: 401 });
  }

  if (req.method === "GET") {
    if (req.headers.get("upgrade") !== "websocket") {
      return new Response("Not Implemented", { status: 501 });
    }

    const { socket, response } = Deno.upgradeWebSocket(req);

    const channel = new BroadcastChannel("message");

    socket.addEventListener("open", async () => {
      // On connect, send this client any pending messages
      for await (const entry of kv.list<string>({ prefix: ["messages"] })) {
        await sendMessage(socket, entry);
      }

      channel.addEventListener("message", async (event) => {
        const entry: MessageEntry = event.data;
        await sendMessage(socket, entry);
      });
    });

    socket.addEventListener("close", () => {
      channel.close();
    });

    return response;
  } else if (req.method === "POST") {
    const key = ["messages", ulid()];
    const value = await req.text();
    const result = await kv.set(key, value);
    if (result.ok) {
      // Tell all clients about this message, but only one will be able to claim it
      // If there are no clients, the message will wait until one connects
      sendChannel.postMessage(
        {
          key,
          value,
          versionstamp: result.versionstamp,
        } satisfies MessageEntry,
      );
    }

    return new Response(null);
  } else {
    return new Response("Method Not Allowed", { status: 405 });
  }
});
