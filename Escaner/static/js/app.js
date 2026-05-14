const $ = id => document.getElementById(id);
const status = $("status");
const details = $("details");

$("complete").onchange = e => {
    $("packs").disabled = e.target.checked;
    if (e.target.checked) $("packs").value = "";
};

["qr", "packs"].forEach(id => {
    $(id).onkeypress = e => {
        if (e.key === "Enter") {
            e.preventDefault();
            submit();
        }
    };
});

$("btn").onclick = submit;
$("qr").focus();

async function submit() {
    const qr = $("qr").value.trim();
    if (!qr) {
        show("⚠️ Escaneá un QR", "error");
        return;
    }
    
    const isComplete = $("complete").checked;
    const packs = isComplete ? 0 : parseInt($("packs").value) || 0;
    
    if (!isComplete && packs < 1) {
        show("⚠️ Ingresá cantidad de packs", "error");
        return;
    }
    
    $("btn").disabled = true;
    $("btn").textContent = "⏳ Procesando...";
    hide();
    
    try {
        const res = await fetch("/api/scan", {
            method: "POST",
            headers: {"Content-Type": "application/json; charset=utf-8"},
            body: JSON.stringify({
                qr: qr,
                is_complete: isComplete,
                packs: packs
            })
        });
        
        const data = await res.json();
        
        if (data.ok) {
            show(`✅ ${data.message} [${data.unit}]`, "success");
            if (data.autofill && data.autofill !== "OK") {
                appendStatus(`🔄 ${data.autofill}`);
            }
            showDetails(data.data);
            reset();
            beep();
        } else {
            const msg = data.error === "DUPLICADO" 
                ? "❌ QR YA REGISTRADO" 
                : `❌ ${data.error}: ${data.message || ''}`;
            show(msg, "error");
            if (data.data) showDetails(data.data);
            errorBeep();
        }
    } catch (err) {
        show("❌ Error de conexión", "error");
        console.error(err);
    } finally {
        $("btn").disabled = false;
        $("btn").textContent = "📤 Registrar";
    }
}

function show(msg, type) {
    status.textContent = msg;
    status.className = type === "success" ? "success" : "error";
    status.classList.remove("d-none");
}

function appendStatus(msg) {
    status.innerHTML += `<br><small>${msg}</small>`;
}

function hide() {
    status.classList.add("d-none");
    details.classList.add("d-none");
}

function showDetails(d) {
    details.textContent = 
        `📦 Serie: ${d.nro_serie}\n` +
        `📦 Producto: ${d.id_producto}\n` +
        `📝 Descripción: ${d.descripcion}\n` +
        `🏷️ Lote: ${d.lote}\n` +
        `📅 Vencimiento: ${d.vencimiento}`;
    details.classList.remove("d-none");
}

function reset() {
    $("qr").value = "";
    $("packs").value = "";
    $("complete").checked = false;
    $("packs").disabled = true;
    $("qr").focus();
}

function beep() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        osc.connect(ctx.destination);
        osc.frequency.value = 800;
        osc.start();
        setTimeout(() => osc.stop(), 80);
    } catch(e) {}
}

function errorBeep() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        osc.connect(ctx.destination);
        osc.frequency.value = 250;
        osc.type = "sawtooth";
        osc.start();
        setTimeout(() => osc.stop(), 150);
    } catch(e) {}
}