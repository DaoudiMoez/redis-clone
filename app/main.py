import asyncio

async def parse_resp(reader):
    """Parses the RESP input from the reader."""
    # Peek at the first byte to determine the type
    first_byte = await reader.read(1)
    if not first_byte:
        raise ValueError("Client disconnected")

    if first_byte == b"*":
        # Array header (e.g., "*2\r\n")
        num_elements = int((await reader.readline()).strip())
        elements = []
        for _ in range(num_elements):
            elements.append(await parse_resp(reader))
        return elements

    elif first_byte == b"$":
        # Bulk string header (e.g., "$5\r\n")
        length = int((await reader.readline()).strip())
        if length == -1:
            return None  # Represents a NULL bulk string
        bulk_string = await reader.readexactly(length)
        await reader.readexactly(2)  # Consume the trailing \r\n
        return bulk_string.decode()

    elif first_byte in {b"+", b"-", b":"}:
        # Simple strings, errors, and integers (e.g., "+OK\r\n", "-ERROR\r\n", ":100\r\n")
        return (await reader.readline()).strip().decode()

    else:
        raise ValueError("Invalid RESP format")

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"Accepted connection from {addr}")

    while True:
        try:
            # Parse the RESP command
            command_parts = await parse_resp(reader)
            if not command_parts or not isinstance(command_parts, list) or len(command_parts) < 1:
                continue

            command = command_parts[0].upper()
            print(f"Received command: {command_parts}")

            if command == "PING":
                response = "+PONG\r\n"
            elif command == "ECHO" and len(command_parts) == 2:
                response = f"+{command_parts[1]}\r\n"
            else:
                response = "-ERROR unknown command\r\n"

            # Send the response
            writer.write(response.encode())
            await writer.drain()
        except (asyncio.IncompleteReadError, ValueError) as e:
            print(f"Error: {e}. Client disconnected.")
            break

    writer.close()
    await writer.wait_closed()
    print(f"Connection from {addr} closed")

async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    addr = server.sockets[0].getsockname()
    print(f"Server is listening on {addr}")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
