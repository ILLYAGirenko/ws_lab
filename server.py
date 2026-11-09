import asyncio
import json
import websockets

# State storage
connected_clients = set()
tasks = []
task_id_counter = 1

async def broadcast_tasks():
    """Sends the updated task list to all connected clients."""
    if connected_clients:
        message = json.dumps({"type": "tasks_update", "tasks": tasks})
        await asyncio.gather(*(client.send(message) for client in connected_clients))

async def handler(websocket):
    """Event handler for each client connection."""
    global task_id_counter
    
    # Register new client
    connected_clients.add(websocket)
    try:
        # Send current task list upon connection
        await websocket.send(json.dumps({"type": "tasks_update", "tasks": tasks}))
        
        # Listen for messages from client
        async for message in websocket:
            data = json.loads(message)
            action = data.get("action")
            
            if action == "get_tasks":
                await websocket.send(json.dumps({"type": "tasks_update", "tasks": tasks}))
                
            elif action == "add_task":
                new_task = {
                    "id": task_id_counter,
                    "title": data.get("title", "New Task"),
                    "status": "active"
                }
                tasks.append(new_task)
                task_id_counter += 1
                await broadcast_tasks()
                
            elif action == "update_task":
                task_id = data.get("task_id")
                new_status = data.get("status")
                for task in tasks:
                    if task["id"] == task_id:
                        task["status"] = new_status
                        break
                await broadcast_tasks()
                
    except websockets.exceptions.ConnectionClosed:
        pass # Client disconnected
    finally:
        # Remove client from active list
        connected_clients.remove(websocket)

async def main():
    """Entry point, runs the server on port 8765."""
    async with websockets.serve(handler, "localhost", 8765):
        print("WebSocket server is running on ws://localhost:8765")
        await asyncio.Future()  # Keep server running

if __name__ == "__main__":
    asyncio.run(main())
