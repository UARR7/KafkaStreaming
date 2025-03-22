

import React, { useEffect, useState } from "react";
import { MessageSquare, ArrowDownToLine, ArrowUpFromLine } from "lucide-react";

// function MessageCard({ message, type }) {
//   return (
//     <div className="slide-in bg-white/10 backdrop-blur-lg rounded-lg p-4 mb-3 border border-white/20 shadow-xl hover:scale-102 transition-transform">
//       <div className="flex items-center gap-2 mb-2">
//         {type === "produced" ? (
//           <ArrowUpFromLine className="text-blue-400" size={20} />
//         ) : (
//           <ArrowDownToLine className="text-green-400" size={20} />
//         )}
//         <span className="text-sm text-gray-300">
//           {new Date(message.timestamp).toLocaleTimeString()}
//         </span>
//       </div>
//       <pre className="text-sm bg-black/20 p-2 rounded overflow-x-auto">
//         {JSON.stringify(message.data, null, 2)}
//       </pre>
//     </div>
//   );
// }

function MessageCard({ message, type }) {
  const { orderId, status } = message.data; // Extract orderId and status

  return (
    <div className="slide-in bg-white/10 backdrop-blur-lg rounded-lg p-4 mb-3 border border-white/20 shadow-xl hover:scale-102 transition-transform">
      <div className="flex items-center gap-2 mb-2">
        {type === "produced" ? (
          <ArrowUpFromLine className="text-blue-400" size={20} />
        ) : (
          <ArrowDownToLine className="text-green-400" size={20} />
        )}
        <span className="text-sm text-gray-300">
          {new Date(message.timestamp).toLocaleTimeString()}
        </span>
      </div>
      <div className="text-white text-sm">
        <p><strong>Order ID:</strong> {orderId}</p>
        <p><strong>Status:</strong> <span className="text-yellow-400">{status}</span></p>
      </div>
    </div>
  );
}

function App() {
  const [ws, setWs] = useState(null);
  const [produced, setProduced] = useState([]);
  const [processed, setProcessed] = useState([]);
  const [selectedOrderId, setSelectedOrderId] = useState("");
  const [selectedStatus, setSelectedStatus] = useState("");

  useEffect(() => {
    const socket = new WebSocket("ws://localhost:8080");

    socket.onopen = () => console.log("✅ WebSocket connected!");
    socket.onerror = (error) => console.error("❌ WebSocket error:", error);
    socket.onclose = () => {
      console.warn("⚠️ WebSocket closed, reconnecting...");
      setTimeout(() => {
        setWs(new WebSocket("ws://localhost:8080"));
      }, 5000);
    };

    // socket.onmessage = (event) => {
    //   const message = JSON.parse(event.data);
    //   const newMessage = {
    //     data: message.data,
    //     timestamp: Date.now(),
    //   };

    //   if (message.type === "produced") {
    //     setProduced((prev) => [newMessage, ...prev]);
    //   } else if (message.type === "consumed") {
    //     setProcessed((prev) => [newMessage, ...prev]);
    //   }
    // };
    socket.onmessage = (event) => {
      const message = JSON.parse(event.data);
      const newMessage = {
        data: message.data,
        timestamp: Date.now(),
      };
    
      if (message.type === "produced") {
        setProduced((prev) => [newMessage, ...prev]);
      } else if (message.type === "consumed" || message.type === "updated") {
        setProcessed((prev) => {
          const existingIndex = prev.findIndex(
            (msg) => msg.data.orderId === newMessage.data.orderId
          );
    
          if (existingIndex !== -1) {
            // Update existing order status
            const updatedOrders = [...prev];
            updatedOrders[existingIndex] = newMessage;
            return updatedOrders;
          } else {
            // Add new order if not found
            return [newMessage, ...prev];
          }
        });
      }
    };
    

    setWs(socket);
    return () => socket.close();
  }, []);

  const updateOrderStatus = () => {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      console.error("❌ WebSocket is not connected");
      return;
    }

    if (!selectedOrderId || !selectedStatus) return;

    ws.send(JSON.stringify({ orderId: Number(selectedOrderId), status: selectedStatus }));
    console.log("✅ Order update sent via WebSocket");
  };

  return (
    <div className="min-h-screen bg-gray-900 text-white relative overflow-hidden">
      <div className="container mx-auto px-4 py-8">
        <div className="flex items-center gap-3 mb-8">
          <MessageSquare className="text-blue-400" size={32} />
          <h1 className="text-3xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-400 to-purple-400">
            Kafka Stream Visualization
          </h1>
        </div>

        <div className="flex gap-4 mb-6">
          <input
            type="number"
            className="bg-gray-800 text-white px-4 py-2 rounded border border-gray-600"
            placeholder="Order ID"
            value={selectedOrderId}
            onChange={(e) => setSelectedOrderId(e.target.value)}
          />
          <select
            className="bg-gray-800 text-white px-4 py-2 rounded border border-gray-600"
            onChange={(e) => setSelectedStatus(e.target.value)}
            value={selectedStatus}
          >
            <option value="">Select Status</option>
            <option value="Processing">Processing</option>
            <option value="Ready">Ready</option>
            <option value="Delivered">Delivered</option>
          </select>
          <button
            className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600"
            onClick={updateOrderStatus}
          >
            Update Order
          </button>
        </div>

        <div className="grid md:grid-cols-2 gap-8">
          <div className="backdrop-blur-sm bg-black/20 rounded-xl p-6">
            <div className="flex items-center gap-2 mb-6">
              <ArrowUpFromLine className="text-blue-400" size={24} />
              <h2 className="text-xl font-semibold text-blue-400">Producer Stream</h2>
            </div>
            <div className="space-y-3 max-h-[600px] overflow-y-auto custom-scrollbar">
              {produced.map((msg, index) => (
                <MessageCard key={index} message={msg} type="produced" />
              ))}
            </div>
          </div>

          <div className="backdrop-blur-sm bg-black/20 rounded-xl p-6">
            <div className="flex items-center gap-2 mb-6">
              <ArrowDownToLine className="text-green-400" size={24} />
              <h2 className="text-xl font-semibold text-green-400">Processed Stream</h2>
            </div>
            <div className="space-y-3 max-h-[600px] overflow-y-auto custom-scrollbar">
              {processed.map((msg, index) => (
                <MessageCard key={index} message={msg} type="consumed" />
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
