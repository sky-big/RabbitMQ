/*global octtree, vec3, Spring */

function searchX(elem, tree, xIncr, xMax) {
    var found = tree.findInRadius(elem.pos, xIncr / 2, 1);
    while (found.length > 0 && elem.pos[octtree.x] + xIncr < xMax) {
        elem.pos[octtree.x] += xIncr;
        found = tree.findInRadius(elem.pos, xIncr / 2, 1);
    }
    return (found.length === 0);
}

function searchY(elem, tree, yIncr) {
    var found = tree.findInRadius(elem.pos, yIncr / 2, 1);
    while (found.length > 0) {
        elem.pos[octtree.y] += yIncr;
        found = tree.findInRadius(elem.pos, yIncr / 2, 1);
    }
}

function bezierMid(startX, startY, ctl1X, ctl1Y, ctl2X, ctl2Y, endX, endY) {
    var start_ctl1X, start_ctl1Y, end_ctl2X, end_ctl2Y, ctl1_ctl2X, ctl1_ctl2Y, mid1X, mid1Y, mid2X, mid2Y;

    start_ctl1X = (startX + ctl1X) / 2;
    start_ctl1Y = (startY + ctl1Y) / 2;

    end_ctl2X = (endX + ctl2X) / 2;
    end_ctl2Y = (endY + ctl2Y) / 2;

    ctl1_ctl2X = (ctl1X + ctl2X) / 2;
    ctl1_ctl2Y = (ctl1Y + ctl2Y) / 2;

    mid1X = (start_ctl1X + ctl1_ctl2X) / 2;
    mid1Y = (start_ctl1Y + ctl1_ctl2Y) / 2;

    mid2X = (end_ctl2X + ctl1_ctl2X) / 2;
    mid2Y = (end_ctl2Y + ctl1_ctl2Y) / 2;

    return [(mid1X + mid2X) / 2, (mid1Y + mid2Y) / 2];
}

function stringifyObject(a) {
    var b, e;
    b = {};
    for (e in a) {
        if (a.hasOwnProperty(e)) {
            if ("object" === typeof a[e]) {
                b[e] = stringifyObject(a[e]);
            } else {
                b[e] = "" + a[e];
            }
        }
    }
    return b;
}

String.prototype.toTitleCase = function () {
    return this.replace(/(^|_)([a-z])/g,
                        function (str, g1, g2, offset, totalStr) {
                            return g1.replace("_", " ") + g2.toUpperCase();
                        });
};

var Consumer = {};
Consumer.render = function (channel, queue, ctx, consumerTag) {
    var yMid, xCtl, dim, mid;
    ctx.beginPath();
    yMid = (channel.yMax + queue.pos[octtree.y]) / 2;
    xCtl = queue.pos[octtree.x];
    ctx.moveTo(channel.pos[octtree.x], channel.yMax);
    ctx.bezierCurveTo(channel.pos[octtree.x], yMid,
                      xCtl, queue.pos[octtree.y] - channel.yInit,
                      xCtl, queue.pos[octtree.y] - queue.fontSize);
    ctx.moveTo(channel.pos[octtree.x], channel.yMax);
    ctx.closePath();
    ctx.stroke();

    dim = ctx.measureText(consumerTag);
    mid = bezierMid(channel.pos[octtree.x], channel.yMax,
                    channel.pos[octtree.x], yMid,
                    xCtl, queue.pos[octtree.y] - channel.yInit,
                    xCtl, queue.pos[octtree.y] - queue.fontSize);
    ctx.textBaseline = "middle";
    ctx.textAlign = "center";
    ctx.fillStyle = "rgba(255, 255, 255, 0.67)";
    ctx.fillRect(mid[0] - (dim.width / 2), mid[1] - (channel.fontSize / 2),
                 dim.width, channel.fontSize);
    ctx.fillStyle = ctx.strokeStyle;
    ctx.fillText(consumerTag, mid[0], mid[1]);

    ctx.beginPath();
    ctx.moveTo(channel.pos[octtree.x], channel.yMax);
    ctx.lineTo(channel.pos[octtree.x] - (channel.fontSize / 2),
               channel.yMax + channel.fontSize);
    ctx.lineTo(channel.pos[octtree.x] + (channel.fontSize / 2),
               channel.yMax + channel.fontSize);
    ctx.closePath();
    ctx.fillStyle = ctx.strokeStyle;
    ctx.fill();
};

var Publisher = {};
Publisher.render = function (channel, exchange, ctx) {
    var yMid, xCtl;
    ctx.beginPath();
    yMid = (channel.yMax + exchange.pos[octtree.y]) / 2;
    xCtl = exchange.pos[octtree.x];
    ctx.moveTo(channel.pos[octtree.x], channel.yMax);
    ctx.bezierCurveTo(channel.pos[octtree.x], yMid,
                      xCtl, exchange.pos[octtree.y] - channel.yInit,
                      xCtl, exchange.pos[octtree.y] - exchange.fontSize);
    ctx.moveTo(channel.pos[octtree.x], channel.yMax);
    ctx.closePath();
    ctx.stroke();

    ctx.beginPath();
    ctx.moveTo(exchange.pos[octtree.x],
               exchange.pos[octtree.y] - exchange.fontSize);
    ctx.lineTo(exchange.pos[octtree.x] - exchange.fontSize / 2,
               exchange.pos[octtree.y] - 2 * exchange.fontSize);
    ctx.lineTo(exchange.pos[octtree.x] + exchange.fontSize / 2,
               exchange.pos[octtree.y] - 2 * exchange.fontSize);
    ctx.closePath();
    ctx.fillStyle = ctx.strokeStyle;
    ctx.fill();

};

function Channel(tree, elem, model) {
    this.name = elem.name;
    this.pos = vec3.create();
    this.findNewPosition(model, tree);

    this.next_pos = vec3.create(this.pos);
    this.mass = 0.1;
    this.velocity = vec3.create();
    this.ideal = { pos : vec3.create() };
    this.disabled = false;
    this.update(elem);
    tree.add(this);
}

Channel.prototype = {
    yInit : 100,
    yIncr : 50,
    xInit : 100,
    xIncr : 50,
    xMax : 200,
    yBoundary : 200,
    attributes : [ 'acks_uncommitted', 'client_flow_blocked', 'confirm', 'connection_details',
                   'consumer_count', 'message_stats', 'messages_unacknowledged',
                   'messages_unconfirmed', 'node', 'number', 'prefetch_count', 'transactional',
                   'user', 'vhost' ],
    pos : vec3.create(),
    fontSize : 12,
    spring : new Spring(),
    details : undefined,
    object_type : 'channel',
    detail_attributes : [ 'name', 'user', 'transactional', 'confirm', 'node', 'vhost',
                          'prefetch_count', 'messages_unacknowledged', 'messages_unconfirmed',
                          'consumer_count', 'client_flow_blocked' ]
};
Channel.prototype.spring.octtreeLimit = 10;
Channel.prototype.spring.octtreeRadius = 500;
Channel.prototype.spring.equilibriumLength = 0;
Channel.prototype.spring.dampingFactor = 0.1;
Channel.prototype.spring.pull = true;
Channel.prototype.spring.push = false;

Channel.prototype.findNewPosition = function (model, tree) {
    this.pos[octtree.x] = this.xInit;
    this.pos[octtree.y] = this.yInit;
    this.pos[octtree.z] = 0;

    while (! searchX(this, tree, this.xIncr, this.xMax)) {
        this.pos[octtree.y] += this.yIncr;
        this.pos[octtree.x] = this.xInit + (this.pos[octtree.y] / 10);
    }

    this.yMin = this.pos[octtree.y];
    this.yMax = this.pos[octtree.y];
};
Channel.prototype.canvasResized = function (canvas) {
    Channel.prototype.xMax = canvas.width;
};
Channel.prototype.update = function (elem) {
    var attr, i;
    for (i = 0; i < this.attributes.length; i += 1) {
        attr = this.attributes[i];
        this[attr] = elem[attr];
    }
};
Channel.prototype.remove = function (tree, model) {
    tree.del(this);
};
Channel.prototype.render = function (model, ctx) {
    var i, dim, consumer, queue, publisher, exchange;
    if (this.disabled) {
        return;
    }
    dim = ctx.measureText(this.name);
    if (model.cull(this.pos[octtree.x] - this.fontSize,
                   this.pos[octtree.y] - (dim.width / 2) - this.fontSize,
                   this.fontSize * 2,
                   dim.width + (this.fontSize * 2))) {
        return;
    }

    this.yMax = this.pos[octtree.y] + (dim.width / 2) + this.fontSize;
    this.yMin = this.pos[octtree.y] - (dim.width / 2) - this.fontSize;

    ctx.beginPath();
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";

    ctx.lineWidth = 2.0;
    ctx.strokeStyle = "black";
    ctx.moveTo(this.pos[octtree.x] - this.fontSize, this.yMin);
    ctx.lineTo(this.pos[octtree.x] + this.fontSize, this.yMin);
    ctx.lineTo(this.pos[octtree.x] + this.fontSize, this.yMax);
    ctx.lineTo(this.pos[octtree.x] - this.fontSize, this.yMax);
    ctx.closePath();
    this.preStroke(model, ctx);

    ctx.save();
    ctx.translate(this.pos[octtree.x], this.pos[octtree.y]);
    ctx.rotate(3 * Math.PI / 2);
    ctx.fillStyle = ctx.strokeStyle;
    ctx.fillText(this.name, 0, 0);
    ctx.restore();

    if (undefined !== this.details) {
        model.resetHighlighted();
        ctx.lineWidth = 2.0;
        if (undefined !== this.details.consumer_details) {
            ctx.strokeStyle = "#00a000";
            for (i = 0; i < this.details.consumer_details.length; i += 1) {
                consumer = this.details.consumer_details[i];
                queue = consumer.queue_details.name;
                if (undefined !== model.queue[queue] && ! model.queue[queue].disabled) {
                    model.setHighlighted(model.queue[queue]);
                    Consumer.render(this, model.queue[queue], ctx, consumer.consumer_tag);
                }
            }
        }

        if (undefined !== this.details.publishes) {
            ctx.strokeStyle = "#0000a0";
            for (i = 0; i < this.details.publishes.length; i += 1) {
                publisher = this.details.publishes[i];
                exchange = publisher.exchange.name;
                if (undefined !== model.exchange[exchange] &&
                    ! model.exchange[exchange].disabled) {
                    model.setHighlighted(model.exchange[exchange]);
                    Publisher.render(this, model.exchange[exchange], ctx);
                }
            }
        }
    }
};
Channel.prototype.preStroke = function (model, ctx) {
};
Channel.prototype.animate = function (elapsed) {
    if (this.yBoundary > this.pos[octtree.y]) {
        this.ideal.pos[octtree.x] = this.pos[octtree.x];
        this.ideal.pos[octtree.y] = this.yInit;
        this.spring.apply(elapsed, this, this.ideal);
    }
};
Channel.prototype.disable = function (model) {
    model.channels_visible -= 1;
};
Channel.prototype.enable = function (model, tree) {
    model.channels_visible += 1;
    this.findNewPosition(model, tree);
};
Channel.prototype.getDetails = function () {
};
Channel.prototype.stringAttributes = function () {
    var obj, i, attName, attNameTitle;
    obj = { Channel : '',
            attributeOrder : ['Channel'] };
    for (i in this.detail_attributes) {
        attName = this.detail_attributes[i];
        attNameTitle = attName.toTitleCase();
        obj.attributeOrder.push(attNameTitle);
        if ("object" === typeof this[attName]) {
            obj[attNameTitle] = stringifyObject(this[attName]);
        } else {
            obj[attNameTitle] = "" + this[attName];
        }
    }

    if (undefined !== this.message_stats) {
        if (undefined !== this.message_stats.publish_details) {
            obj.attributeOrder.push('Publish Rate (msgs/sec)');
            obj['Publish Rate (msgs/sec)'] = "" + Math.round(this.message_stats.publish_details.rate);
        }

        if (undefined !== this.message_stats.deliver_get_details) {
            obj.attributeOrder.push('Delivery and Get Rate (msgs/sec)');
            obj['Delivery and Get Rate (msgs/sec)'] = "" + Math.round(this.message_stats.deliver_get_details.rate);
        }

        if (undefined !== this.message_stats.ack_details) {
            obj.attributeOrder.push('Delivery Acknowledgement Rate (acks/sec)');
            obj['Delivery Acknowledgement Rate (acks/sec)'] = "" + Math.round(this.message_stats.ack_details.rate);
        }
    }

    return obj;
};
Channel.prototype.url = function () {
    return "/channels/" + encodeURIComponent(this.name);
};
Channel.prototype.navigateTo = function () {
    document.location = "../#" + this.url();
};

function Exchange(tree, elem, model) {
    this.name = elem.name;
    this.pos = vec3.create();
    this.findNewPosition(model, tree);
    this.next_pos = vec3.create(this.pos);
    this.mass = 0.1;
    this.velocity = vec3.create();
    this.ideal = { pos : vec3.create() };
    this.disabled = false;
    this.bindings_outbound = { exchange : {}, queue : {} };
    this.bindings_inbound = {};
    this.update(elem);
    tree.add(this);
}

Exchange.prototype = {
    yInit : 250,
    yIncr : 50,
    xInit : 100,
    xBoundary : 200,
    attributes : [ 'arguments', 'auto_delete', 'durable', 'internal', 'type',
                   'message_stats_out', 'message_stats_in', 'vhost' ],
    pos : vec3.create(),
    fontSize : 12,
    spring : new Spring(),
    details : undefined,
    object_type : 'exchange',
    detail_attributes : [ 'name', 'type', 'durable', 'auto_delete', 'internal', 'arguments', 'vhost' ]
};
Exchange.prototype.spring.octtreeLimit = 10;
Exchange.prototype.spring.octtreeRadius = 500;
Exchange.prototype.spring.equilibriumLength = 0;
Exchange.prototype.spring.dampingFactor = 0.1;
Exchange.prototype.spring.pull = true;
Exchange.prototype.spring.push = false;

Exchange.prototype.findNewPosition = function (model, tree) {
    this.pos[octtree.x] = this.xInit;
    this.pos[octtree.y] = this.yInit;
    this.pos[octtree.z] = 0;

    searchY(this, tree, this.yIncr);

    this.xMin = this.pos[octtree.x];
    this.xMax = this.pos[octtree.x];
};
Exchange.prototype.canvasResized = function (canvas) {
    Exchange.prototype.xInit = canvas.width / 6;
    Exchange.prototype.xBoundary = 2 * canvas.width / 6;
};
Exchange.prototype.update = function (elem) {
    var attr, i;
    for (i = 0; i < this.attributes.length; i += 1) {
        attr = this.attributes[i];
        this[attr] = elem[attr];
    }
};
Exchange.prototype.remove = function (tree, model) {
    tree.del(this);
};
Exchange.prototype.render = function (model, ctx) {
    var i, dim, channel;
    if (this.disabled) {
        return;
    }
    for (i in this.bindings_outbound.exchange) {
        this.bindings_outbound.exchange[i].render(model, ctx);
    }
    if (model.rendering.queue.enabled) {
        for (i in this.bindings_outbound.queue) {
            this.bindings_outbound.queue[i].render(model, ctx);
        }
    }
    dim = ctx.measureText(this.name);
    if (model.cull(this.pos[octtree.x] - (dim.width / 2) - this.fontSize,
                   this.pos[octtree.y] - this.fontSize,
                   dim.width + (2 * this.fontSize),
                   2 * this.fontSize)) {
        return;
    }

    ctx.beginPath();
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";

    ctx.lineWidth = 2.0;
    ctx.strokeStyle = "black";

    ctx.arc(this.pos[octtree.x] - (dim.width / 2), this.pos[octtree.y],
            this.fontSize, Math.PI / 2, 3 * Math.PI / 2, false);
    ctx.lineTo(this.pos[octtree.x] + (dim.width / 2), this.pos[octtree.y] -
               this.fontSize);

    ctx.arc(this.pos[octtree.x] + (dim.width / 2), this.pos[octtree.y],
            this.fontSize, 3 * Math.PI / 2, Math.PI / 2, false);
    ctx.closePath();

    this.preStroke(model, ctx);

    ctx.fillStyle = ctx.strokeStyle;
    ctx.fillText(this.name, this.pos[octtree.x], this.pos[octtree.y]);

    this.xMin = this.pos[octtree.x] - (dim.width / 2) - this.fontSize;
    this.xMax = this.pos[octtree.x] + (dim.width / 2) + this.fontSize;

    if (undefined !== this.details) {
        model.resetHighlighted();
        ctx.lineWidth = 2.0;
        ctx.strokeStyle = "#00a000";
        if (undefined !== this.details.incoming) {
            for (i = 0; i < this.details.incoming.length; i += 1) {
                channel = this.details.incoming[i].channel_details.name;
                if (undefined !== model.channel[channel] && ! model.channel[channel].disabled) {
                    model.setHighlighted(model.channel[channel]);
                    Publisher.render(model.channel[channel], this, ctx);
                }
            }
        }

        for (i in this.bindings_outbound.queue) {
            model.setHighlighted(model.queue[this.bindings_outbound.queue[i].destination]);
        }
        for (i in this.bindings_outbound.exchange) {
            model.setHighlighted(model.exchange[this.bindings_outbound.exchange[i].destination]);
        }
        for (i in this.bindings_inbound) {
            model.setHighlighted(model.exchange[this.bindings_inbound[i].source]);
        }

    }
};
Exchange.prototype.preStroke = function (model, ctx) {
};
Exchange.prototype.animate = function (elapsed) {
    if (this.xBoundary > this.pos[octtree.x]) {
        this.ideal.pos[octtree.x] = this.xInit;
        this.ideal.pos[octtree.y] = this.pos[octtree.y];
        this.spring.apply(elapsed, this, this.ideal);
    }
};
Exchange.prototype.disable = function (model) {
    model.exchanges_visible -= 1;
};
Exchange.prototype.enable = function (model, tree) {
    model.exchanges_visible += 1;
    this.findNewPosition(model, tree);
};
Exchange.prototype.getDetails = function () {
};
Exchange.prototype.stringAttributes = function () {
    var obj, i, attName, attNameTitle;
    obj = { Exchange : '',
            attributeOrder : ['Exchange'] };
    for (i in this.detail_attributes) {
        attName = this.detail_attributes[i];
        attNameTitle = attName.toTitleCase();
        obj.attributeOrder.push(attNameTitle);
        if ("object" === typeof this[attName]) {
            obj[attNameTitle] = stringifyObject(this[attName]);
        } else {
            obj[attNameTitle] = "" + this[attName];
        }
    }

    obj.attributeOrder.push('Outgoing Queue Bindings');
    obj['Outgoing Queue Bindings'] = "" + Object.keys(this.bindings_outbound.queue).length;

    obj.attributeOrder.push('Outgoing Exchange Bindings');
    obj['Outgoing Exchange Bindings'] = "" + Object.keys(this.bindings_outbound.exchange).length;

    obj.attributeOrder.push('Incoming Exchange Bindings');
    obj['Incoming Exchange Bindings'] = "" + Object.keys(this.bindings_inbound).length;

    if (undefined !== this.message_stats_in &&
        undefined !== this.message_stats_in.publish_details) {
        obj.attributeOrder.push('Message Incoming Rate (msgs/sec)');
        obj['Message Incoming Rate (msgs/sec)'] = "" + Math.round(this.message_stats_in.publish_details.rate);
    }

    if (undefined !== this.message_stats_out &&
        undefined !== this.message_stats_out.publish_details) {
        obj.attributeOrder.push('Message Outgoing Rate (msgs/sec)');
        obj['Message Outgoing Rate (msgs/sec)'] = "" + Math.round(this.message_stats_out.publish_details.rate);
    }

    return obj;
};
Exchange.prototype.url = function () {
    var name;
    if (this.name === "") {
        name = "amq.default";
    } else {
        name = this.name;
    }
    return "/exchanges/" + encodeURIComponent(this.vhost) +
        "/" + encodeURIComponent(name);
};
Exchange.prototype.navigateTo = function () {
    document.location = "../#" + this.url();
};

function Queue(tree, elem, model) {
    this.name = elem.name;
    this.pos = vec3.create();
    this.findNewPosition(model, tree);
    this.next_pos = vec3.create(this.pos);
    this.mass = 0.1;
    this.velocity = vec3.create();
    this.ideal = { pos : vec3.create() };
    this.disabled = false;
    this.bindings_inbound = {};
    this.update(elem);
    tree.add(this);
}

Queue.prototype = {
    yInit : 250,
    yIncr : 50,
    xInit : 400,
    xBoundary : 300,
    attributes : [ 'arguments', 'auto_delete', 'durable', 'messages',
                   'messages_ready', 'messages_unacknowledged', 'message_stats',
                   'node', 'owner_pid_details', 'vhost', 'memory', 'consumers' ],
    pos : vec3.create(),
    fontSize : 12,
    spring : new Spring(),
    details : undefined,
    object_type : 'queue',
    detail_attributes : [ 'name', 'durable', 'auto_delete', 'arguments', 'node', 'vhost',
                          'messages_ready', 'messages_unacknowledged', 'consumers', 'memory' ]
};
Queue.prototype.spring.octtreeLimit = 10;
Queue.prototype.spring.octtreeRadius = 500;
Queue.prototype.spring.equilibriumLength = 0;
Queue.prototype.spring.dampingFactor = 0.1;
Queue.prototype.spring.pull = true;
Queue.prototype.spring.push = false;

Queue.prototype.findNewPosition = function (model, tree) {
    this.pos[octtree.x] = this.xInit;
    this.pos[octtree.y] = this.yInit;
    this.pos[octtree.z] = 0;

    searchY(this, tree, this.yIncr);

    this.xMin = this.pos[octtree.x];
    this.xMax = this.pos[octtree.x];
};
Queue.prototype.canvasResized = function (canvas) {
    Queue.prototype.xInit = 5 * canvas.width / 6;
    Queue.prototype.xBoundary = 4 * canvas.width / 6;
};
Queue.prototype.update = function (elem) {
    var attr, i;
    for (i = 0; i < this.attributes.length; i += 1) {
        attr = this.attributes[i];
        this[attr] = elem[attr];
    }
};
Queue.prototype.remove = function (tree, model) {
    tree.del(this);
};
Queue.prototype.render = function (model, ctx) {
    var text, dim, i, channel;
    if (this.disabled) {
        return;
    }
    text = this.name + " (" + this.messages_ready + ", " +
        this.messages_unacknowledged + ")";
    dim = ctx.measureText(text);
    if (model.cull(this.pos[octtree.x] - (dim.width / 2) - this.fontSize,
                   this.pos[octtree.y] - this.fontSize,
                   dim.width + (2 * this.fontSize),
                   2 * this.fontSize)) {
        return;
    }
    ctx.beginPath();
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";

    ctx.lineWidth = 2.0;
    ctx.strokeStyle = "black";
    ctx.moveTo(this.pos[octtree.x] - (dim.width / 2) - this.fontSize,
               this.pos[octtree.y] - this.fontSize);
    ctx.lineTo(this.pos[octtree.x] + (dim.width / 2) + this.fontSize,
               this.pos[octtree.y] - this.fontSize);
    ctx.lineTo(this.pos[octtree.x] + (dim.width / 2) + this.fontSize,
               this.pos[octtree.y] + this.fontSize);
    ctx.lineTo(this.pos[octtree.x] - (dim.width / 2) - this.fontSize,
               this.pos[octtree.y] + this.fontSize);
    ctx.closePath();

    this.preStroke(model, ctx);

    ctx.fillStyle = ctx.strokeStyle;
    ctx.fillText(text, this.pos[octtree.x], this.pos[octtree.y]);

    this.xMin = this.pos[octtree.x] - (dim.width / 2) - this.fontSize;
    this.xMax = this.pos[octtree.x] + (dim.width / 2) + this.fontSize;

    if (undefined !== this.details && undefined !== this.details.consumer_details) {
        model.resetHighlighted();
        ctx.lineWidth = 2.0;
        ctx.strokeStyle = "#0000a0";
        for (i = 0; i < this.details.consumer_details.length; i += 1) {
            channel = this.details.consumer_details[i].channel_details.name;
            if (undefined !== model.channel[channel] && ! model.channel[channel].disabled) {
                model.setHighlighted(model.channel[channel]);
                Consumer.render(model.channel[channel], this, ctx,
                                this.details.consumer_details[i].consumer_tag);
            }
        }
        for (i in this.bindings_inbound) {
            model.setHighlighted(model.exchange[this.bindings_inbound[i].source]);
        }
    }
};
Queue.prototype.preStroke = function (model, ctx) {
};
Queue.prototype.animate = function (elapsed) {
    if (this.xBoundary < this.pos[octtree.x]) {
        this.ideal.pos[octtree.x] = this.xInit;
        this.ideal.pos[octtree.y] = this.pos[octtree.y];
        this.spring.apply(elapsed, this, this.ideal);
    }
};
Queue.prototype.disable = function (model) {
    model.queues_visible -= 1;
};
Queue.prototype.enable = function (model, tree) {
    model.queues_visible += 1;
    this.findNewPosition(model, tree);
};
Queue.prototype.getDetails = function () {
};
Queue.prototype.stringAttributes = function () {
    var obj, i, attName, attNameTitle;
    obj = { Queue : '',
            attributeOrder : ['Queue'] };
    for (i in this.detail_attributes) {
        attName = this.detail_attributes[i];
        attNameTitle = attName.toTitleCase();
        obj.attributeOrder.push(attNameTitle);
        if ("object" === typeof this[attName]) {
            obj[attNameTitle] = stringifyObject(this[attName]);
        } else {
            obj[attNameTitle] = "" + this[attName];
        }
    }

    obj.attributeOrder.push('Incoming Exchange Bindings');
    obj['Incoming Exchange Bindings'] = "" + Object.keys(this.bindings_inbound).length;

    if (undefined !== this.message_stats) {
        if (undefined !== this.message_stats.publish_details) {
            obj.attributeOrder.push('Message Incoming Rate (msgs/sec)');
            obj['Message Incoming Rate (msgs/sec)'] = "" + Math.round(this.message_stats.publish_details.rate);
        }

        if (undefined !== this.message_stats.deliver_get_details) {
            obj.attributeOrder.push('Delivery and Get Rate (msgs/sec)');
            obj['Delivery and Get Rate (msgs/sec)'] = "" + Math.round(this.message_stats.deliver_get_details.rate);
        }

        if (undefined !== this.message_stats.ack_details) {
            obj.attributeOrder.push('Delivery Acknowledgement Rate (acks/sec)');
            obj['Delivery Acknowledgement Rate (acks/sec)'] = "" + Math.round(this.message_stats.ack_details.rate);
        }
    }

    return obj;
};
Queue.prototype.url = function () {
    return "/queues/" + encodeURIComponent(this.vhost) +
        "/" + encodeURIComponent(this.name);
};
Queue.prototype.navigateTo = function () {
    document.location = "../#" + this.url();
};

function Binding(elems) {
    this.keys = {};
    this.set(elems);
    var elem = elems.shift();
    this.source = elem.source;
    this.destination_type = elem.destination_type;
    this.destination = elem.destination;
}
Binding.prototype = {
    attributes : [ 'arguments' ],
    offset : 150,
    fontSize : 12,
    loopOffset : 50,
    object_type : 'binding'
};
Binding.prototype.set = function (elems) {
    var i, elem, attr, j;
    this.keys = {};
    for (i = 0; i < elems.length; i += 1) {
        elem = elems[i];
        this.keys[elem.routing_key] = {};
        for (j = 0; j < this.attributes.length; j += 1) {
            attr = this.attributes[j];
            this.keys[elem.routing_key][attr] = elem[attr];
        }
    }
};
Binding.prototype.render = function (model, ctx) {
    var source, destination, xMid, xCtl1, xCtl2, yCtl1, yCtl2, xMin, yMin, xMax, yMax;
    source = model.exchange[this.source];
    if (this.destination_type === "exchange") {
        destination = model.exchange[this.destination];
    } else {
        destination = model.queue[this.destination];
    }
    if (undefined === source || undefined === destination) {
        return;
    }
    if (source.disabled || destination.disabled) {
        return;
    }
    xMid = (source.xMax + destination.xMin) / 2;
    xCtl1 = xMid > (source.xMax + this.offset) ? xMid : source.xMax + this.offset;
    xCtl2 = xMid < (destination.xMin - this.offset) ? xMid
    : destination.xMin - this.offset;
    yCtl1 = destination === source ? source.pos[octtree.y] - this.loopOffset : source.pos[octtree.y];
    yCtl2 = destination === source ? destination.pos[octtree.y] - this.loopOffset : destination.pos[octtree.y];
    xMin = Math.min(source.xMax, xCtl2);
    yMin = Math.min(yCtl1, yCtl2);
    xMax = Math.max(destination.xMin, xCtl1);
    yMax = Math.max(source.pos[octtree.y], destination.pos[octtree.y]);
    if (model.cull(xMin, yMin, xMax - xMin, yMax - yMin)) {
        return;
    }

    ctx.beginPath();
    ctx.lineWidth = 1.0;
    ctx.strokeStyle = "black";
    ctx.moveTo(source.xMax, source.pos[octtree.y]);
    ctx.bezierCurveTo(xCtl1, yCtl1, xCtl2, yCtl2, destination.xMin,
                      destination.pos[octtree.y]);
    ctx.moveTo(destination.xMin, destination.pos[octtree.y] + 1);
    ctx.bezierCurveTo(xCtl2, yCtl2 + 1, xCtl1, yCtl1 + 1, source.xMax,
                      source.pos[octtree.y] + 1);
    ctx.moveTo(source.xMax, source.pos[octtree.y]);
    this.preStroke(source, destination, model, ctx);

    // draw an arrow head
    ctx.beginPath();
    ctx.moveTo(destination.xMin, destination.pos[octtree.y]);
    ctx.lineTo(destination.xMin - this.fontSize, destination.pos[octtree.y] +
               (this.fontSize / 2));
    ctx.lineTo(destination.xMin - this.fontSize, destination.pos[octtree.y] -
               (this.fontSize / 2));
    ctx.closePath();
    ctx.fillStyle = ctx.strokeStyle;
    ctx.fill();
};
Binding.prototype.preStroke = function (source, destination, model, ctx) {
};

function Model() {
    this.exchange = {};
    this.exchanges_visible = 0;
    this.queue = {};
    this.queues_visible = 0;
    this.channel = {};
    this.channels_visible = 0;
    this.connection = {};
    this.vhost = {};
    this.rendering = { exchange   : { enabled   : true,
                                      on_enable : {} },
                       queue      : { enabled   : true,
                                      on_enable : {} },
                       channel    : { enabled   : true,
                                      on_enable : {} },
                       connection : { enabled   : true,
                                      on_enable : {} }
                     };
    this.highlighted = { exchange   : {},
                         queue      : {},
                         channel    : {},
                         connection : {} };
}

Model.prototype.permitted_exchanges_visible = 10;
Model.prototype.permitted_queues_visible = 10;
Model.prototype.permitted_channels_visible = 10;

Model.prototype.rebuild = function (tree, configuration) {
    var elem, matched, i, binding, bindings, source, src, destination_type, j, src1, destination, dest, dest_type;

    // Channels
    matched = {};
    for (i = 0; i < configuration.channels.length; i += 1) {
        elem = configuration.channels[i];
        if (undefined === this.channel[elem.name]) {
            this.channel[elem.name] = new Channel(tree, elem, this);
            this.channels_visible += 1;
            if ((this.channels_visible >
                 this.permitted_channels_visible) ||
                ! this.rendering.channel.enabled) {
                this.disable(this.channel[elem.name], tree);
            }
        } else {
            this.channel[elem.name].update(elem);
        }
        matched[elem.name] = true;
    }
    for (i in this.channel) {
        if (undefined === matched[i]) {
            elem = this.channel[i];
            delete this.channel[i];
            elem.remove(tree, this);
            if (! elem.disabled) {
                this.channels_visible -= 1;
            }
        }
    }

    // Exchanges
    matched = {};
    for (i = 0; i < configuration.exchanges.length; i += 1) {
        elem = configuration.exchanges[i];
        if (undefined === this.exchange[elem.name]) {
            this.exchange[elem.name] = new Exchange(tree, elem, this);
            this.exchanges_visible += 1;
            if (elem.name.slice(0, 4) === "amq." ||
                (this.exchanges_visible >
                 this.permitted_exchanges_visible) ||
                ! this.rendering.exchange.enabled) {
                this.disable(this.exchange[elem.name], tree);
            }
        } else {
            this.exchange[elem.name].update(elem);
        }
        matched[elem.name] = true;
    }
    for (i in this.exchange) {
        if (undefined === matched[i]) {
            elem = this.exchange[i];
            delete this.exchange[i];
            elem.remove(tree, this);
            if (! elem.disabled) {
                this.exchanges_visible -= 1;
            }
        }
    }

    // Queues
    matched = {};
    for (i = 0; i < configuration.queues.length; i += 1) {
        elem = configuration.queues[i];
        if (undefined === this.queue[elem.name]) {
            this.queue[elem.name] = new Queue(tree, elem, this);
            this.queues_visible += 1;
            if ((this.queues_visible >
                 this.permitted_queues_visible) ||
                ! this.rendering.queue.enabled) {
                this.disable(this.queue[elem.name], tree);
                delete this.rendering.queue.on_enable[elem.name];
            }
        } else {
            this.queue[elem.name].update(elem);
        }
        matched[elem.name] = true;
    }
    for (i in this.queue) {
        if (undefined === matched[i]) {
            elem = this.queue[i];
            delete this.queue[i];
            elem.remove(tree, this);
            if (! elem.disabled) {
                this.queues_visible -= 1;
            }
        }
    }

    // Bindings
    bindings = {};
    for (i = 0; i < configuration.bindings.length; i += 1) {
        elem = configuration.bindings[i];
        if (undefined === this.exchange[elem.source] ||
            undefined === this[elem.destination_type][elem.destination]) {
            continue;
        }
        if (undefined === bindings[elem.source]) {
            bindings[elem.source] = { exchange : {}, queue : {} };
        }
        source = bindings[elem.source];
        if (undefined === source[elem.destination_type][elem.destination]) {
            source[elem.destination_type][elem.destination] = new Array(elem);
        } else {
            source[elem.destination_type][elem.destination].push(elem);
        }
    }

    for (source in bindings) {
        src = this.exchange[source].bindings_outbound;
        i = bindings[source];
        for (destination_type in i) {
            j = i[destination_type];
            src1 = src[destination_type];
            for (destination in j) {
                dest = this[destination_type][destination].bindings_inbound;
                if (undefined === src1[destination]) {
                    src1[destination] = new Binding(j[destination]);
                } else {
                    src1[destination].set(j[destination]);
                }
                binding = src1[destination];
                if (undefined === dest[source]) {
                    dest[source] = binding;
                }
            }
        }
    }
    for (src in this.exchange) {
        for (dest_type in this.exchange[src].bindings_outbound) {
            for (dest in this.exchange[src].bindings_outbound[dest_type]) {
                binding = this.exchange[src].bindings_outbound[dest_type][dest];
                if (undefined === bindings[binding.source] ||
                    undefined === bindings[binding.source][binding.destination_type] ||
                    undefined === bindings[binding.source][binding.destination_type][binding.destination]) {
                    delete this.exchange[src].bindings_outbound[dest_type][dest];
                    if (undefined !== this[binding.destination_type][binding.destination]) {
                        delete this[binding.destination_type][binding.destination].bindings_inbound[binding.source];
                    }
                }
            }
        }
    }
    bindings = undefined;

    // vhosts
    matched = {};
    for (i = 0; i < configuration.vhosts.length; i += 1) {
        elem = configuration.vhosts[i];
        if (undefined === this.vhost[elem.name]) {
            this.vhost[elem.name] = elem;
            this.vhost_add(elem);
        }
        matched[elem.name] = true;
    }
    for (i in this.vhost) {
        if (undefined === matched[i]) {
            this.vhost_remove(this.vhost[i]);
            delete this.vhost[i];
        }
    }

    matched = undefined;
};
Model.prototype.disable = function (elem, tree) {
    elem.disable(this);
    tree.del(elem);
    elem.disabled = true;
    elem.details = undefined;
};
Model.prototype.enable = function (elem, tree) {
    elem.enable(this, tree);
    tree.add(elem);
    elem.disabled = false;
    elem.details = undefined;
};
Model.prototype.render = function (ctx) {
    var i;
    if (this.rendering.exchange.enabled) {
        for (i in this.exchange) {
            this.exchange[i].render(this, ctx);
        }
    }
    if (this.rendering.queue.enabled) {
        for (i in this.queue) {
            this.queue[i].render(this, ctx);
        }
    }
    if (this.rendering.channel.enabled) {
        for (i in this.channel) {
            this.channel[i].render(this, ctx);
        }
    }
};
Model.prototype.cull = function (xMin, yMin, width, height) {
    return false;
};
Model.prototype.vhost_add = function (elem) {
};
Model.prototype.vhost_del = function (elem) {
};
Model.prototype.resetHighlighted = function () {
    this.highlighted = { exchange   : {},
                         queue      : {},
                         channel    : {},
                         connection : {} };
};
Model.prototype.setHighlighted = function (elem) {
    if (undefined !== elem) {
        this.highlighted[elem.object_type][elem.name] = elem;
    }
};
Model.prototype.isHighlighted = function (elem) {
    return ((undefined !== elem) && (undefined !== this.highlighted[elem.object_type][elem.name]));
};
