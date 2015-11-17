/*global vec3 */

var octtree = {};
octtree.top_nw = 0;
octtree.top_ne = 1;
octtree.top_se = 2;
octtree.top_sw = 3;
octtree.bot_nw = 4;
octtree.bot_ne = 5;
octtree.bot_se = 6;
octtree.bot_sw = 7;
octtree.children = [ octtree.top_nw, octtree.top_ne, octtree.top_se,
                     octtree.top_sw, octtree.bot_nw, octtree.bot_ne, octtree.bot_se,
                     octtree.bot_sw ];
octtree.firstChildId = 0;
octtree.lastChildId = 7;
octtree.x = 0;
octtree.y = 1;
octtree.z = 2;
octtree.randoms = [];
octtree.randomIndex = 0;
octtree.i = 0;

for (octtree.i = 0; octtree.i < 100; octtree.i += 1) {
    octtree.randoms.push(Math.random());
}

function Octtree(xMin, xMax, yMin, yMax, zMin, zMax, parent, childId) {
    this.xMin = xMin;
    this.xMax = xMax;
    this.yMin = yMin;
    this.yMax = yMax;
    this.zMin = zMin;
    this.zMax = zMax;
    this.parent = parent;
    this.childId = childId;
    if (undefined !== childId && childId !== octtree.lastChildId &&
        undefined !== parent) {
        this.nextSiblingId = childId + 1;
    }

    this.xMid = xMin + (xMax - xMin) / 2;
    this.yMid = yMin + (yMax - yMin) / 2;
    this.zMid = zMin + (zMax - zMin) / 2;
}

Octtree.prototype.isEmpty = function () {
    return (undefined === this[octtree.firstChildId]) &&
        (undefined === this.value);
};

Octtree.prototype.hasChildren = function () {
    return undefined !== this[octtree.firstChildId];
};

Octtree.prototype.hasValue = function () {
    return undefined !== this.value;
};

Octtree.prototype.add = function (value) {
    return octtree.add(this, value);
};

Octtree.prototype.del = function (value) {
    return octtree.del(this, value);
};

Octtree.prototype.update = function () {
    return octtree.update(this);
};

Octtree.prototype.findInRadius = function (pos, radius, limit) {
    return octtree.findInRadius(this, pos, radius, limit);
};

Octtree.prototype.size = function () {
    return octtree.size(this);
};

octtree.findNode = function (tree, pos) {
    while (true) {
        if (pos[octtree.x] < tree.xMin || tree.xMax <= pos[octtree.x] ||
            pos[octtree.y] < tree.yMin || tree.yMax <= pos[octtree.y] ||
            pos[octtree.z] < tree.zMin || tree.zMax <= pos[octtree.z]) {
            if (undefined === tree.parent) {
                return undefined;
            } else {
                tree = tree.parent;
                continue;
            }
        }

        if (tree.hasChildren()) {
            if (pos[octtree.x] < tree.xMid) {
                if (pos[octtree.y] < tree.yMid) {
                    if (pos[octtree.z] < tree.zMid) {
                        tree = tree[octtree.bot_sw];
                    } else {
                        tree = tree[octtree.bot_nw];
                    }
                } else {
                    if (pos[octtree.z] < tree.zMid) {
                        tree = tree[octtree.top_sw];
                    } else {
                        tree = tree[octtree.top_nw];
                    }
                }
            } else {
                if (pos[octtree.y] < tree.yMid) {
                    if (pos[octtree.z] < tree.zMid) {
                        tree = tree[octtree.bot_se];
                    } else {
                        tree = tree[octtree.bot_ne];
                    }
                } else {
                    if (pos[octtree.z] < tree.zMid) {
                        tree = tree[octtree.top_se];
                    } else {
                        tree = tree[octtree.top_ne];
                    }
                }
            }
        } else {
            return tree;
        }
    }
};

octtree.add = function (tree, value) {
    tree = octtree.findNode(tree, value.pos);
    if (undefined === tree) {
        return undefined;
    } else {
        var displaced;
        while (undefined !== value) {
            if (tree.hasValue()) {
                if (tree.value.pos[octtree.x] === value.pos[octtree.x] &&
                    tree.value.pos[octtree.y] === value.pos[octtree.y] &&
                    tree.value.pos[octtree.z] === value.pos[octtree.z]) {
                    tree.value = value;
                    value = undefined;
                } else {
                    displaced = value; // make sure we add our new value last
                    value = tree.value;
                    tree.value = undefined;

                    tree[octtree.top_nw] = new Octtree(tree.xMin, tree.xMid,
                                                       tree.yMid, tree.yMax, tree.zMid, tree.zMax, tree,
                                                       octtree.top_nw);
                    tree[octtree.top_ne] = new Octtree(tree.xMid, tree.xMax,
                                                       tree.yMid, tree.yMax, tree.zMid, tree.zMax, tree,
                                                       octtree.top_ne);
                    tree[octtree.top_se] = new Octtree(tree.xMid, tree.xMax,
                                                       tree.yMid, tree.yMax, tree.zMin, tree.zMid, tree,
                                                       octtree.top_se);
                    tree[octtree.top_sw] = new Octtree(tree.xMin, tree.xMid,
                                                       tree.yMid, tree.yMax, tree.zMin, tree.zMid, tree,
                                                       octtree.top_sw);

                    tree[octtree.bot_nw] = new Octtree(tree.xMin, tree.xMid,
                                                       tree.yMin, tree.yMid, tree.zMid, tree.zMax, tree,
                                                       octtree.bot_nw);
                    tree[octtree.bot_ne] = new Octtree(tree.xMid, tree.xMax,
                                                       tree.yMin, tree.yMid, tree.zMid, tree.zMax, tree,
                                                       octtree.bot_ne);
                    tree[octtree.bot_se] = new Octtree(tree.xMid, tree.xMax,
                                                       tree.yMin, tree.yMid, tree.zMin, tree.zMid, tree,
                                                       octtree.bot_se);
                    tree[octtree.bot_sw] = new Octtree(tree.xMin, tree.xMid,
                                                       tree.yMin, tree.yMid, tree.zMin, tree.zMid, tree,
                                                       octtree.bot_sw);
                    tree = octtree.findNode(tree, value.pos);
                }
            } else {
                tree.value = value;
                value = displaced;
                displaced = undefined;
                if (undefined !== value) {
                    tree = octtree.findNode(tree, value.pos);
                }
            }
        }
        return tree;
    }
};

octtree.del = function (tree, value) {
    tree = octtree.findNode(tree, value.pos);
    if (undefined === tree || (!tree.hasValue())) {
        return tree;
    }
    if (tree.value.pos[octtree.x] === value.pos[octtree.x] &&
        tree.value.pos[octtree.y] === value.pos[octtree.y] &&
        tree.value.pos[octtree.z] === value.pos[octtree.z]) {
        tree.value = undefined;
        tree = tree.parent;
        var valCount, nonEmptyChild, child, i;
        while (undefined !== tree) {
            valCount = 0;
            for (i = 0; i < octtree.children.length; i += 1) {
                child = octtree.children[i];
                if (!tree[child].isEmpty()) {
                    valCount += 1;
                    nonEmptyChild = tree[child];
                }
            }
            if (0 === valCount) {
                for (i = 0; i < octtree.children.length; i += 1) {
                    child = octtree.children[i];
                    tree[child] = undefined;
                }
                tree = tree.parent;
            } else if (1 === valCount) {
                if (nonEmptyChild.hasValue()) {
                    for (i = 0; i < octtree.children.length; i += 1) {
                        child = octtree.children[i];
                        tree[child] = undefined;
                    }
                    tree.value = nonEmptyChild.value;
                    tree = tree.parent;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }
    return tree;
};

octtree.next = function (tree) {
    while (undefined !== tree) {
        if (undefined !== tree.nextSiblingId) {
            return tree.parent[tree.nextSiblingId];
        } else {
            tree = tree.parent;
        }
    }
    return undefined;
};

octtree.defined = function (a, b) {
    if (undefined === a) {
        return b;
    } else {
        return a;
    }
};

octtree.update = function (tree) {
    var root, parent, v, movedValues, i;
    root = tree;
    parent = root.parent;
    root.parent = undefined; // do this to stop next going up past tree

    movedValues = [];
    while (undefined !== tree) {
        if (tree.hasValue()) {
            v = tree.value;
            if (v.next_pos[octtree.x] < tree.xMin ||
                tree.xMax <= v.next_pos[octtree.x] ||
                v.next_pos[octtree.y] < tree.yMin ||
                tree.yMax <= v.next_pos[octtree.y] ||
                v.next_pos[octtree.z] < tree.zMin ||
                tree.zMax <= v.next_pos[octtree.z]) {
                movedValues.push(tree.value);
            } else {
                vec3.set(v.next_pos, v.pos);
            }
            tree = octtree.next(tree);
        } else if (tree.hasChildren()) {
            tree = tree[octtree.firstChildId];
        } else {
            tree = octtree.next(tree);
        }
    }

    root.parent = parent;
    tree = root;
    for (i = 0; i < movedValues.length; i += 1) {
        v = movedValues[i];
        tree = octtree.defined(tree.del(v), tree);
        vec3.set(v.next_pos, v.pos);
        tree = octtree.defined(tree.add(v), tree);
    }

    return root;
};

octtree.findInRadius = function (tree, pos, radius, limit) {
    var acc, radiusSq, worklist, x_p_r, x_m_r, y_p_r, y_m_r, z_p_r, z_m_r, xd, yd, zd;
    acc = [];
    radiusSq = radius * radius;
    worklist = [tree];
    tree = undefined;

    x_p_r = 0;
    x_m_r = 0;
    y_p_r = 0;
    y_m_r = 0;
    z_p_r = 0;
    z_m_r = 0;

    while (0 < worklist.length && (undefined === limit || limit > acc.length)) {
        tree = worklist.shift();

        if (tree.isEmpty()) {
            continue;
        }

        if (tree.hasValue()) {
            xd = Math.abs(tree.value.pos[octtree.x] - pos[octtree.x]);
            yd = Math.abs(tree.value.pos[octtree.y] - pos[octtree.y]);
            zd = Math.abs(tree.value.pos[octtree.z] - pos[octtree.z]);
            xd *= xd;
            yd *= yd;
            zd *= zd;
            if ((xd + yd + zd) <= radiusSq) {
                acc.push(tree);
            }
            continue;
        }

        x_p_r = pos[octtree.x] + radius;
        x_m_r = pos[octtree.x] - radius;
        y_p_r = pos[octtree.y] + radius;
        y_m_r = pos[octtree.y] - radius;
        z_p_r = pos[octtree.z] + radius;
        z_m_r = pos[octtree.z] - radius;

        if (x_p_r < tree.xMin || tree.xMax <= x_m_r || y_p_r < tree.yMin ||
            tree.yMax <= y_m_r || z_p_r < tree.zMin || tree.zMax <= z_m_r) {
            continue;
        }

        if (x_m_r < tree.xMid) {
            if (y_m_r < tree.yMid) {
                if (z_m_r < tree.zMid) {
                    octtree.randomPush(worklist, tree[octtree.bot_sw]);
                }
                if (tree.zMid <= z_p_r) {
                    octtree.randomPush(worklist, tree[octtree.bot_nw]);
                }
            }
            if (tree.yMid <= y_p_r) {
                if (z_m_r < tree.zMid) {
                    octtree.randomPush(worklist, tree[octtree.top_sw]);
                }
                if (tree.zMid <= z_p_r) {
                    octtree.randomPush(worklist, tree[octtree.top_nw]);
                }
            }
        }
        if (tree.xMid <= x_p_r) {
            if (y_m_r < tree.yMid) {
                if (z_m_r < tree.zMid) {
                    octtree.randomPush(worklist, tree[octtree.bot_se]);
                }
                if (tree.zMid <= z_p_r) {
                    octtree.randomPush(worklist, tree[octtree.bot_ne]);
                }
            }
            if (tree.yMid <= y_p_r) {
                if (z_m_r < tree.zMid) {
                    octtree.randomPush(worklist, tree[octtree.top_se]);
                }
                if (tree.zMid <= z_p_r) {
                    octtree.randomPush(worklist, tree[octtree.top_ne]);
                }
            }
        }
    }
    return acc;
};

octtree.size = function (tree) {
    var count, root, parent;
    root = 0;
    root = tree;
    parent = root.parent;
    root.parent = undefined; // stop the traversal going above us.

    while (undefined !== tree) {
        if (tree.hasValue()) {
            count += 1;
            tree = octtree.next(tree);
        } else if (tree.hasChildren()) {
            tree = tree[octtree.firstChildId];
        } else {
            tree = octtree.next(tree);
        }
    }

    root.parent = parent;
    return count;
};

octtree.create = function (xMin, xMax, yMin, yMax, zMin, zMax) {
    return new Octtree(xMin, xMax, yMin, yMax, zMin, zMax, undefined, undefined);
};

octtree.randomPush = function (ary, e) {
    if (octtree.nextRandom() > 0.5) {
        ary.push(e);
    } else {
        ary.unshift(e);
    }
    return ary;
};

octtree.nextRandom = function () {
    var r = octtree.randoms[octtree.randomIndex];
    octtree.randomIndex += 1;
    if (octtree.randomIndex === octtree.randoms.length) {
        octtree.randomIndex = 0;
    }
    return r;
};
