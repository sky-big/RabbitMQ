/*global octtree, vec3, Model, Channel, Exchange, Queue, Binding, Newton, Spring, window */
/*jslint browser: true, devel: true */

var tree = octtree.create(0, 10000, 0, 1000000, -0.5, 2);
var updatePeriod = 1000; // 1 second

var configuration, detailsInFlight, mouseDragOffsetVec, hoveringOver, dragging, selectedVhost, ctx, canvas, tick, mouseMove, setCanvasMousemove, requestAnimFrame;

var client = new XMLHttpRequest();

var detailsClient = new XMLHttpRequest();

var model = new Model();
var mousePos = vec3.create();
var mouseDown = false;
var highlight = "#ffffc0";
var faded = "#c0c0c0";

var eta = 0.1;
var max_v = 100000;

var newton = new Newton();
var spring = new Spring();
spring.octtreeRadius = 500;
spring.equilibriumLength = 50;
spring.dampingFactor = 0.01;
spring.pull = false;
spring.push = true;

var rendering = true;
var lastTime = 0;

var fontSize = 12;
Exchange.prototype.fontSize = fontSize;
Queue.prototype.fontSize = fontSize;
Binding.prototype.fontSize = fontSize;
var canvasLeft = 0;
var canvasTop = 0;
var scrollLeft = 0;
var scrollTop = 0;
var clientWidth = 0;
var clientHeight = 0;


/******************************************************************************
 * Fetching details from the broker                                           *
 ******************************************************************************/

function update() {
    if (undefined === selectedVhost) {
        client.open("GET", "../api/all");
    } else {
        client.open("GET", "../api/all/" + encodeURIComponent(selectedVhost));
    }
    client.send();
}

function updateReady() {
    if (client.readyState === 4 && client.status === 200) {
        setTimeout(update, updatePeriod);
        configuration = JSON.parse(client.responseText);
        model.rebuild(tree, configuration);
        if (!rendering) {
            lastTime = 0;
            requestAnimFrame(tick);
        }
    }
}
client.onreadystatechange = updateReady;

function getDetails() {
    detailsInFlight = this;
    detailsClient.abort();
    detailsClient.open("GET", "../api" + this.url());
    detailsClient.send();
}

Channel.prototype.getDetails = getDetails;
Exchange.prototype.getDetails = getDetails;
Queue.prototype.getDetails = getDetails;

function repeatGetDetails() {
    if (undefined !== hoveringOver) {
        hoveringOver.getDetails();
    }
}

function flattenAtts(a) {
    if ("string" === typeof a) {
        return a;
    } else {
        var str, e;
        str = "{";
        for (e in a) {
            if (a.hasOwnProperty(e)) {
                str += "" + e + ": " + flattenAtts(a[e]) + ", ";
            }
        }
        return str.replace(/(, )?$/, "}");
    }
}

function setDetails(elem) {
    var details, strAtts, visibleRows, columns, column, str, attName, i;
    details = document.getElementById("details");
    if (undefined === elem) {
        details.innerHTML = "";
        detailsInFlight = undefined;
    } else {
        strAtts = elem.stringAttributes();
        visibleRows = Math.floor(details.clientHeight / 16); // line-height + padding;
        columns = Math.ceil(strAtts.attributeOrder.length / visibleRows);
        column = 0;
        str = "<table><tr>";
        for (i in strAtts.attributeOrder) {
            column += 1;
            attName = strAtts.attributeOrder[i];
            if (undefined !== strAtts[attName]) {
                str += "<th>" + attName + "</th><td>" + flattenAtts(strAtts[attName]) + "</td>";
            } else {
                str += "<th>" + attName + "</th><td></td>";
            }
            if (column === columns) {
                column = 0;
                str += "</tr><tr>";
            }
        }
        str += "</tr></table>";
        document.getElementById("details").innerHTML = str;
    }
}

function detailsUpdateReady() {
    if (detailsClient.readyState === 4 &&
        detailsClient.status === 200 &&
        undefined !== hoveringOver &&
        undefined !== detailsInFlight &&
        hoveringOver.object_type === detailsInFlight.object_type &&
        hoveringOver.name === detailsInFlight.name) {
        try {
            var details = JSON.parse(detailsClient.responseText);
            if (undefined !== details.name &&
                details.name === detailsInFlight.name) {
                model[detailsInFlight.object_type][detailsInFlight.name].details = details;
                setDetails(model[detailsInFlight.object_type][detailsInFlight.name]);
                setTimeout(repeatGetDetails, updatePeriod);
            }
        } catch (err) {
            // We probably cancelled it as we were receiving data.
            model[detailsInFlight.object_type][detailsInFlight.name].details = undefined;
            window.console.info("" + err);
        }
    }
}
detailsClient.onreadystatechange = detailsUpdateReady;


/******************************************************************************
 * Rendering / animation                                                      *
 ******************************************************************************/

requestAnimFrame = (function () {
    return (this.requestAnimationFrame ||
            this.webkitRequestAnimationFrame ||
            this.mozRequestAnimationFrame ||
            this.oRequestAnimationFrame ||
            this.msRequestAnimationFrame ||
            function (/* function FrameRequestCallback */ callback, /* DOMElement Element */ element) {
                setTimeout(callback, 1000 / 60);
            });
})();

function recordMousePos(e) {
    var x, y;
    x = e.pageX;
    y = e.pageY;
    x = (x - canvasLeft) + scrollLeft;
    y = (y - canvasTop) + scrollTop;
    mousePos[octtree.x] = x;
    mousePos[octtree.y] = y;
}

mouseMove = function (e) {
    recordMousePos(e);
    canvas.onmousemove = undefined;
    setTimeout(setCanvasMousemove, 10);
};

setCanvasMousemove = function () {
    if (rendering) {
        canvas.onmousemove = mouseMove;
    }
};

function resizeCanvas() {
    var e;
    if (undefined !== canvas) {
        canvas.width = canvas.parentNode.offsetWidth;
        canvas.height = canvas.parentNode.offsetHeight;
        Channel.prototype.canvasResized(canvas);
        Exchange.prototype.canvasResized(canvas);
        Queue.prototype.canvasResized(canvas);
        clientWidth = canvas.width;
        clientHeight = canvas.height;
        e = canvas.parentNode;
        while (undefined !== e && null !== e) {
            if (undefined !== e.clientHeight && undefined !== e.clientWidth &&
                e.clientHeight > 0 && e.clientWidth > 0) {
                clientHeight = Math.min(clientHeight, e.clientHeight);
                clientWidth = Math.min(clientWidth, e.clientWidth);
            }
            e = e.parentNode;
        }
        canvasLeft = 0;
        canvasTop = 0;
        e = canvas.parentNode;
        while (undefined !== e && null !== e) {
            if (undefined !== e.offsetLeft && undefined !== e.offsetTop) {
                canvasLeft += e.offsetLeft;
                canvasTop += e.offsetTop;
            }
            e = e.parentNode;
        }
        if (undefined !== hoveringOver && undefined !== hoveringOver.details) {
            setDetails(hoveringOver);
        }
    }
}

function canvasScroll() {
    scrollLeft = 0;
    scrollTop = 0;
    var e = canvas.parentNode;
    while (undefined !== e && null !== e) {
        if (undefined !== e.scrollLeft && undefined !== e.scrollTop) {
            scrollLeft += e.scrollLeft;
            scrollTop += e.scrollTop;
        }
        e = e.parentNode;
    }
}

function clamp(elem) {
    var x_vel_abs, y_vel_abs;
    x_vel_abs = Math.abs(elem.velocity[octtree.x]);
    y_vel_abs = Math.abs(elem.velocity[octtree.y]);
    if (0 !== x_vel_abs && eta > x_vel_abs) {
        elem.velocity[octtree.x] = 0;
    } else if (max_v < x_vel_abs) {
        elem.velocity[octtree.x] = max_v * (x_vel_abs / elem.velocity[octtree.x]);
    }
    if (0 !== y_vel_abs && eta > y_vel_abs) {
        elem.velocity[octtree.y] = 0;
    } else if (max_v < y_vel_abs) {
        elem.velocity[octtree.y] = max_v * (y_vel_abs / elem.velocity[octtree.y]);
    }
    if (elem.next_pos[octtree.x] < 1) {
        elem.next_pos[octtree.x] = 1;
        elem.velocity[octtree.x] = 0;
    }
    if (elem.next_pos[octtree.y] < 1) {
        elem.next_pos[octtree.y] = 1;
        elem.velocity[octtree.y] = 0;
    }
    if (elem.next_pos[octtree.x] >= canvas.width) {
        elem.next_pos[octtree.x] = canvas.width - 1;
    }
    if (elem.next_pos[octtree.y] >= (canvas.height - 100)) {
        canvas.height += 100;
    }
}

function initCanvas() {
    resizeCanvas();
    setCanvasMousemove();
    canvas.onmousedown = function (e) {
        recordMousePos(e);
        if (e.shiftKey && undefined !== hoveringOver) {
            model.disable(hoveringOver, tree);
            mouseDown = false;
            hoveringOver = undefined;
            dragging = undefined;
        } else {
            mouseDown = true;
            mouseDragOffsetVec = undefined;
        }
    };
    canvas.ondblclick = function (e) {
        recordMousePos(e);
        if (undefined !== hoveringOver) {
            hoveringOver.navigateTo();
        }
    };
    canvas.onmouseup = function (e) {
        recordMousePos(e);
        mouseDown = false;
        mouseDragOffsetVec = undefined;
        dragging = undefined;
    };
    try {
        ctx = canvas.getContext("2d");
    } catch (e) {
    }
    if (!ctx) {
        alert("Could not initialise 2D canvas. Change browser?");
    }
}

function drawScene() {
    ctx.font = "" + fontSize + "px sans-serif";
    ctx.clearRect(scrollLeft, scrollTop, clientWidth, clientHeight);
    ctx.lineWidth = 1.0;
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.strokeStyle = "black";
    model.render(ctx);
}

function animate() {
    var timeNow, elapsed, e, i;
    timeNow = new Date().getTime();
    if (lastTime !== 0) {
        elapsed = (timeNow - lastTime) / 10000;
        for (i in model.exchange) {
            e = model.exchange[i];
            if ((undefined === dragging || dragging !== e) && ! e.disabled) {
                e.animate(elapsed);
                newton.update(elapsed, e);
                spring.update(elapsed, tree, e);
                clamp(e);
            }
        }
        for (i in model.channel) {
            e = model.channel[i];
            if ((undefined === dragging || dragging !== e) && ! e.disabled) {
                e.animate(elapsed);
                newton.update(elapsed, e);
                spring.update(elapsed, tree, e);
                clamp(e);
            }
        }
        for (i in model.queue) {
            e = model.queue[i];
            if ((undefined === dragging || dragging !== e) && ! e.disabled) {
                e.animate(elapsed);
                newton.update(elapsed, e);
                spring.update(elapsed, tree, e);
                clamp(e);
            }
        }
        tree.update();
    }
    lastTime = timeNow;
}

tick = function () {
    drawScene();
    animate();
    if (rendering) {
        requestAnimFrame(tick);
    }
};

function visualisationStart() {
    canvas = document.getElementById("topology_canvas");
    initCanvas();
    update();
    requestAnimFrame(tick);
}

// Used to start/stop doing work when we gain/lose focus
function enableRendering() {
    lastTime = 0;
    rendering = true;
    setCanvasMousemove();
    requestAnimFrame(tick);
}

function disableRendering() {
    canvas.onmousemove = undefined;
    rendering = false;
}


/******************************************************************************
 * Model callbacks for rendering                                              *
 ******************************************************************************/

function draggable(model, ctx) {
    var inPath = ctx.isPointInPath(mousePos[octtree.x], mousePos[octtree.y]);
    if ((inPath && undefined === hoveringOver) || dragging === this || hoveringOver === this) {
        ctx.fillStyle = highlight;
        ctx.fill();

        if (hoveringOver !== this) {
            this.getDetails();
        }

        hoveringOver = this;
        if (mouseDown) {
            dragging = this;
            if (undefined === mouseDragOffsetVec) {
                mouseDragOffsetVec = vec3.create(this.pos);
                vec3.subtract(mouseDragOffsetVec, mousePos);
            }
            vec3.set(mousePos, this.next_pos);
            vec3.add(this.next_pos, mouseDragOffsetVec);
            this.velocity = vec3.create();
            clamp(this);
        } else if (!inPath) {
            if (undefined !== hoveringOver) {
                hoveringOver.details = undefined;
            }
            if (detailsInFlight === this) {
                setDetails(undefined);
            }
            dragging = undefined;
            hoveringOver = undefined;
            mouseDragOffsetVec = undefined;
        }
    } else {
        ctx.fillStyle = "white";
        ctx.fill();
    }
    if (undefined !== hoveringOver && hoveringOver !== this && ! model.isHighlighted(this)) {
        ctx.strokeStyle = faded;
    }
    ctx.stroke();
}

Channel.prototype.preStroke = draggable;
Exchange.prototype.preStroke = draggable;
Queue.prototype.preStroke = draggable;

Binding.prototype.preStroke = function (source, destination, model, ctx) {
    var drawBindingKeys, xMid, yMid, bindingKey, k, dim;
    drawBindingKeys = false;
    if (undefined === hoveringOver) {
        drawBindingKeys = ctx.isPointInPath(mousePos[octtree.x], mousePos[octtree.y]);
    } else {
        if (hoveringOver === source) {
            ctx.strokeStyle = "#0000a0";
            drawBindingKeys = true;
        } else if (hoveringOver === destination) {
            ctx.strokeStyle = "#00a000";
            drawBindingKeys = true;
        } else {
            ctx.strokeStyle = faded;
        }
    }
    ctx.stroke();

    if (drawBindingKeys) {
        xMid = (source.xMax + destination.xMin) / 2;
        yMid = source === destination ? source.pos[octtree.y] - this.loopOffset + fontSize
            : (source.pos[octtree.y] + destination.pos[octtree.y]) / 2;
        bindingKey = "";
        for (k in this.keys) {
            bindingKey += ", " + k;
        }
        bindingKey = bindingKey.slice(2);
        dim = ctx.measureText(bindingKey);

        ctx.textBaseline = "middle";
        ctx.textAlign = "center";
        ctx.fillStyle = "rgba(255, 255, 255, 0.67)";
        ctx.fillRect(xMid - (dim.width / 2), yMid - (this.fontSize / 2),
                     dim.width, this.fontSize);
        ctx.fillStyle = ctx.strokeStyle;
        ctx.fillText(bindingKey, xMid, yMid);
    }
};

function frustumCull(xMin, yMin, width, height) {
    return ((yMin > (scrollTop + clientHeight)) ||
            ((yMin + height) < scrollTop) ||
            (xMin > (scrollLeft + clientWidth)) ||
            ((xMin + width) < scrollLeft));
}
Model.prototype.cull = frustumCull;


/******************************************************************************
 * Showing / hiding / removing resources                                      *
 ******************************************************************************/

function selectInsertAlphabetical(selectElem, optionElem) {
    var preceding, i;
    for (i = 0; i < selectElem.options.length; i += 1) {
        if (optionElem.text < selectElem.options[i].text) {
            preceding = selectElem.options[i];
            break;
        }
    }
    selectElem.add(optionElem, preceding);
    return selectElem.options;
}

function show(hiddenElemId, model, type) {
    var i, e, hidden;
    if (model.rendering[type].enabled) {
        hidden = document.getElementById(hiddenElemId);
        for (i = 0; i < hidden.options.length; i += 1) {
            e = hidden.options[i];
            if (e.selected) {
                model.enable(model[type][e.value], tree);
                hidden.remove(i);
                i -= 1;
            }
        }
    }
}

function showChannels() {
    show("hidden_channels", model, 'channel');
}

function showExchanges() {
    show("hidden_exchanges", model, 'exchange');
}

function showQueues() {
    show("hidden_queues", model, 'queue');
}

// Called when the resource is enabled from being hidden
function enable_fun(type, postFun) {
    return function (model, tree) {
        if (model.rendering[type].enabled) {
            delete model.rendering[type].on_enable[this.name];
        }
        this.remove = Object.getPrototypeOf(this).remove;
        this.postFun = postFun;
        this.postFun(model, tree);
    };
}

Channel.prototype.enable = enable_fun('channel', Channel.prototype.enable);
Exchange.prototype.enable = enable_fun('exchange', Exchange.prototype.enable);
Queue.prototype.enable = enable_fun('queue', Queue.prototype.enable);


// Called when the item is removed and the item is disabled
function remove_disabled_fun(hiddenElemId, postFun) {
    return function (tree, model) {
        var hidden, i;
        hidden = document.getElementById(hiddenElemId);
        for (i = 0; i < hidden.options.length; i += 1) {
            if (hidden.options[i].value === this.name) {
                hidden.remove(i);
                break;
            }
        }
        model.enable(this, tree);
        this.postFun = postFun;
        this.postFun(tree, model);
    };
}

function disable_fun(hiddenElemId, type, postFun) {
    return function (model) {
        if (detailsInFlight === this) {
            setDetails(undefined);
        }
        var optionElem = document.createElement('option');
        optionElem.text = '"' + this.name + '"';
        if (undefined !== model.rendering[type].on_enable[this.name]) {
            optionElem.text += ' *';
        }
        optionElem.value = this.name;
        selectInsertAlphabetical(document.getElementById(hiddenElemId), optionElem);
        this.remove = remove_disabled_fun(hiddenElemId, this.remove);
        this.postFun = postFun;
        this.postFun(model);
    };
}

Channel.prototype.disable =
    disable_fun("hidden_channels", 'channel', Channel.prototype.disable);
Exchange.prototype.disable =
    disable_fun("hidden_exchanges", 'exchange', Exchange.prototype.disable);
Queue.prototype.disable =
    disable_fun("hidden_queues", 'queue', Queue.prototype.disable);

// Called when the resource is deleted / vanishes on the broker
function remove_fun(postFun, type) {
    return function (tree, model) {
        if (undefined !== hoveringOver && this === hoveringOver) {
            hoveringOver = undefined;
            dragging = undefined;
        }
        delete model.rendering[type].on_enable[this.name];
        if (this === detailsInFlight) {
            setDetails(undefined);
        }
        this.postFun = postFun;
        this.postFun(tree, model);
    };
}

Channel.prototype.remove = remove_fun(Channel.prototype.remove, 'channel');
Queue.prototype.remove = remove_fun(Queue.prototype.remove, 'queue');
Exchange.prototype.remove = remove_fun(Exchange.prototype.remove, 'exchange');

function toggleRendering(hiddenElemId, showButtonElemId, type) {
    var hidden, i, e;
    model.rendering[type].enabled = !model.rendering[type].enabled;
    if (model.rendering[type].enabled) {
        hidden = document.getElementById(hiddenElemId);
        for (i = 0; i < hidden.options.length; i += 1) {
            e = hidden.options[i].value;
            if (undefined !== model.rendering[type].on_enable[e]) {
                model.enable(model[type][e], tree);
                hidden.remove(i);
                i -= 1;
            }
        }
        document.getElementById(showButtonElemId).disabled = false;
    } else {
        for (i in model[type]) {
            if (! model[type][i].disabled) {
                model.rendering[type].on_enable[model[type][i].name] = true;
                model.disable(model[type][i], tree);
            }
        }
        document.getElementById(showButtonElemId).disabled = true;
    }
    return true;
}

function displayHelp() {
    document.getElementById('help').style.display = 'block';
}

function hideHelp() {
    document.getElementById('help').style.display = 'none';
}


/******************************************************************************
 * VHost                                                                      *
 ******************************************************************************/

Model.prototype.vhost_add = function (vhost) {
    var optionElem, options;
    optionElem = document.createElement('option');
    optionElem.text = vhost.name;
    optionElem.value = vhost.name;
    options =
        selectInsertAlphabetical(document.getElementById('vhosts'), optionElem);
    if (options.length === 1) {
        selectedVhost = options[0].value;
        options[0].selected = true;
    }
};

Model.prototype.vhost_remove = function (vhost) {
    var elem, i;
    elem = document.getElementById('vhosts');
    for (i = 0; i < elem.options.length; i += 1) {
        if (elem.options[i].value === vhost.name) {
            elem.remove(i);
            break;
        }
    }
};

function vhostChanged() {
    var elem, i, e, j;
    elem = document.getElementById('vhosts');
    for (i = 0; i < elem.options.length; i += 1) {
        if (elem.options[i].selected && selectedVhost !== elem.options[i].value) {
            selectedVhost = elem.options[i].value;
            for (e in ['channel', 'exchange', 'queue']) {
                for (j in model[e]) {
                    model[e][j].remove(tree, model);
                }
            }
            break;
        }
    }
}
