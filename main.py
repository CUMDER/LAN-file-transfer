#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
局域网文件传输工具
功能：发送/接收文件和文件夹，显示传输速率和进度，支持 TCP / P2P 模式，深色浅色主题，GitHub 更新检查。
版本：1.0.0
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, Toplevel
import socket
import threading
import struct
import os
import json
import time
import tempfile
import zipfile
import sys
import queue
import urllib.request
import urllib.error
import platform
import subprocess

#  ================== 全局配置 ==================
VERSION = "1.0.0"
CONFIG_FILE = "config.json"
DEFAULT_CONFIG = {
    "mode": "tcp",           # tcp 或 p2p（P2P 模式使用 UDP 示例）
    "theme": "light",        # light 或 dark
    "port": 5000,
    "download_dir": os.path.expanduser("~") + "/Downloads/LANFileTransfer",
    "github_repo": "CUMDER/LAN-file-transfer"   # 请改为实际仓库（用于检查更新）
}

# 传输协议头格式：
# 文件名长度 (4字节, 大端) + 文件名 (UTF-8) + 文件大小 (8字节, 大端) + is_zip (1字节, 0=单文件, 1=zip包)
HEADER_FMT = "!I Q B"
HEADER_SIZE = struct.calcsize(HEADER_FMT)  # 4+8+1=13

#  ================== 工具函数 ==================
def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            # 用默认值补全缺失的键
            for k, v in DEFAULT_CONFIG.items():
                if k not in cfg:
                    cfg[k] = v
            return cfg
        except Exception:
            pass
    return DEFAULT_CONFIG.copy()

def save_config(cfg):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

#  ================== 传输协议实现 ==================
class TCPTransfer:
    """基于 TCP 的可靠传输"""
    @staticmethod
    def send_file(host, port, filepath, progress_queue):
        """发送文件/zip包，进度信息通过 progress_queue 传出"""
        filename = os.path.basename(filepath)
        # 判断是否为 zip（文件夹会先打包）
        is_zip = 1 if filepath.endswith('.zip') else 0
        file_size = os.path.getsize(filepath)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            sock.connect((host, port))

            # 发送头部
            name_bytes = filename.encode('utf-8')
            header = struct.pack(HEADER_FMT, len(name_bytes), file_size, is_zip)
            sock.sendall(header + name_bytes)

            # 发送文件内容
            sent = 0
            start_time = time.time()
            last_update = start_time
            last_sent = 0
            with open(filepath, 'rb') as f:
                while True:
                    chunk = f.read(65536)
                    if not chunk:
                        break
                    sock.sendall(chunk)
                    sent += len(chunk)
                    now = time.time()
                    # 每 0.5 秒报告进度
                    if now - last_update >= 0.5 or sent == file_size:
                        elapsed = now - start_time
                        speed = sent / elapsed if elapsed > 0 else 0
                        percent = (sent / file_size * 100) if file_size > 0 else 100
                        progress_queue.put({
                            "type": "progress",
                            "filename": filename,
                            "percent": percent,
                            "speed": speed,
                            "size": file_size,
                            "sent": sent
                        })
                        last_update = now

            # 完成
            progress_queue.put({
                "type": "done",
                "filename": filename,
                "success": True
            })

    @staticmethod
    def start_server(port, save_dir, progress_queue, stop_event=None, socket_holder=None):
        """启动 TCP 服务器接收文件，支持通过 stop_event 停止"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", port))
        server.listen(1)
        server.settimeout(1.0)  # 非阻塞等待，便于检查停止事件
        # 将服务器 socket 传出，供外部关闭
        if socket_holder is not None:
            socket_holder.append(server)

        progress_queue.put({"type": "status", "msg": f"正在监听 TCP 端口 {port} ..."})

        try:
            while not (stop_event and stop_event.is_set()):
                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    continue
                except Exception as e:
                    progress_queue.put({"type": "error", "msg": f"接受连接异常: {e}"})
                    break

                with conn:
                    conn.settimeout(10)
                    # 接收头部
                    header = b''
                    while len(header) < HEADER_SIZE:
                        chunk = conn.recv(HEADER_SIZE - len(header))
                        if not chunk:
                            break
                        header += chunk
                    if len(header) < HEADER_SIZE:
                        progress_queue.put({"type": "error", "msg": "接收头部失败"})
                        continue

                    name_len, file_size, is_zip = struct.unpack(HEADER_FMT, header)
                    # 接收文件名
                    name_bytes = b''
                    while len(name_bytes) < name_len:
                        chunk = conn.recv(name_len - len(name_bytes))
                        if not chunk:
                            break
                        name_bytes += chunk
                    filename = name_bytes.decode('utf-8', errors='replace')
                    if not filename:
                        filename = "received_file"

                    # 准备保存路径
                    os.makedirs(save_dir, exist_ok=True)
                    if is_zip:
                        recv_path = os.path.join(save_dir, filename + ".zip")
                    else:
                        recv_path = os.path.join(save_dir, filename)

                    # 接收数据
                    received = 0
                    start_time = time.time()
                    last_update = start_time
                    with open(recv_path, 'wb') as f:
                        while received < file_size:
                            # 检查停止事件，允许中断接收
                            if stop_event and stop_event.is_set():
                                progress_queue.put({"type": "status", "msg": "接收已取消"})
                                raise Exception("接收被用户取消")
                            try:
                                chunk = conn.recv(min(65536, file_size - received))
                            except socket.timeout:
                                continue
                            if not chunk:
                                break
                            f.write(chunk)
                            received += len(chunk)
                            now = time.time()
                            if now - last_update >= 0.5 or received == file_size:
                                elapsed = now - start_time
                                speed = received / elapsed if elapsed > 0 else 0
                                percent = (received / file_size * 100) if file_size > 0 else 100
                                progress_queue.put({
                                    "type": "progress",
                                    "filename": filename,
                                    "percent": percent,
                                    "speed": speed,
                                    "size": file_size,
                                    "sent": received
                                })
                                last_update = now

                    # 如果是 zip，解压并删除 zip
                    if received == file_size and is_zip:
                        extract_dir = os.path.join(save_dir, os.path.splitext(filename)[0])
                        os.makedirs(extract_dir, exist_ok=True)
                        try:
                            with zipfile.ZipFile(recv_path, 'r') as zf:
                                zf.extractall(extract_dir)
                            os.remove(recv_path)
                            progress_queue.put({"type": "status", "msg": f"已解压文件夹到 {extract_dir}"})
                        except Exception as e:
                            progress_queue.put({"type": "error", "msg": f"解压失败: {e}"})

                    progress_queue.put({
                        "type": "done",
                        "filename": filename,
                        "success": True
                    })
        finally:
            server.close()
            progress_queue.put({"type": "status", "msg": "TCP 服务器已关闭"})
            # 清理 socket_holder
            if socket_holder and server in socket_holder:
                socket_holder.remove(server)

class P2PTransfer:
    @staticmethod
    def send_file(host, port, filepath, progress_queue):
        # 极其简化的 UDP 发送，未处理丢包，仅供示意
        filename = os.path.basename(filepath)
        is_zip = 1 if filepath.endswith('.zip') else 0
        file_size = os.path.getsize(filepath)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2)

        name_bytes = filename.encode('utf-8')
        header = struct.pack(HEADER_FMT, len(name_bytes), file_size, is_zip) + name_bytes
        sock.sendto(header, (host, port))

        sent = 0
        start_time = time.time()
        with open(filepath, 'rb') as f:
            while sent < file_size:
                chunk = f.read(512)  # UDP 不宜过大
                sock.sendto(chunk, (host, port))
                sent += len(chunk)
                # 等待 ACK（忽略）
                try:
                    ack, _ = sock.recvfrom(16)
                except socket.timeout:
                    pass

                now = time.time()
                if now - start_time > 0.5:
                    progress_queue.put({
                        "type": "progress",
                        "filename": filename,
                        "percent": (sent / file_size) * 100,
                        "speed": sent / (now - start_time) if now > start_time else 0,
                        "size": file_size,
                        "sent": sent
                    })
                    start_time = now
        sock.close()
        progress_queue.put({"type": "done", "filename": filename, "success": True})

    @staticmethod
    def start_server(port, save_dir, progress_queue, stop_event=None, socket_holder=None):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", port))
        sock.settimeout(1.0)
        if socket_holder is not None:
            socket_holder.append(sock)

        progress_queue.put({"type": "status", "msg": f"正在监听 UDP 端口 {port}..."})

        try:
            while not (stop_event and stop_event.is_set()):
                try:
                    data, addr = sock.recvfrom(4096)
                except socket.timeout:
                    continue
                if len(data) < HEADER_SIZE:
                    continue
                name_len, file_size, is_zip = struct.unpack(HEADER_FMT, data[:HEADER_SIZE])
                name_bytes = data[HEADER_SIZE:HEADER_SIZE + name_len] if len(data) > HEADER_SIZE else b''
                filename = name_bytes.decode('utf-8', errors='replace') or "received_file"
                recv_path = os.path.join(save_dir, filename)
                sock.sendto(b'ACK', addr)

                received = 0
                start_time = time.time()
                with open(recv_path, 'wb') as f:
                    while received < file_size:
                        if stop_event and stop_event.is_set():
                            break
                        try:
                            chunk, _ = sock.recvfrom(1024)
                            sock.sendto(b'ACK', addr)
                        except socket.timeout:
                            continue
                        f.write(chunk)
                        received += len(chunk)
                        now = time.time()
                        if now - start_time >= 0.5:
                            progress_queue.put({
                                "type": "progress",
                                "filename": filename,
                                "percent": (received / file_size) * 100,
                                "speed": received / (now - start_time),
                                "size": file_size,
                                "sent": received
                            })
                            start_time = now
                if received == file_size and is_zip:
                    extract_dir = os.path.join(save_dir, os.path.splitext(filename)[0])
                    os.makedirs(extract_dir, exist_ok=True)
                    with zipfile.ZipFile(recv_path, 'r') as zf:
                        zf.extractall(extract_dir)
                    os.remove(recv_path)
                progress_queue.put({"type": "done", "filename": filename, "success": True})
        finally:
            sock.close()
            progress_queue.put({"type": "status", "msg": "UDP 服务器已关闭"})
            if socket_holder and sock in socket_holder:
                socket_holder.remove(sock)

#  ================== 主应用程序界面 ==================
class LANFileTransferApp:
    def __init__(self, root):
        self.root = root
        self.root.title("局域网文件传输 v" + VERSION)
        self.root.geometry("800x600")
        self.root.minsize(700, 500)
        self.config = load_config()
        self.progress_queue = queue.Queue()
        self.files_to_send = []  # 待发送文件/文件夹路径列表

        # 接收相关变量
        self.recv_thread = None
        self.recv_socket = None
        self.recv_stop_event = threading.Event()

        self.setup_theme()
        self.setup_menu()
        self.create_widgets()
        self.apply_theme()
        self.update_progress_loop()
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    # ---------- 主题 ----------
    def setup_theme(self):
        self.style = ttk.Style()
        self.style.theme_use('clam')  # 便于自定义颜色
        self.bg_color = "#f0f0f0" if self.config["theme"] == "light" else "#2d2d2d"
        self.fg_color = "#000000" if self.config["theme"] == "light" else "#ffffff"
        self.widget_bg = "#ffffff" if self.config["theme"] == "light" else "#3c3c3c"
        self.root.configure(bg=self.bg_color)

    def apply_theme(self):
        # 通用样式
        self.style.configure('.', background=self.bg_color, foreground=self.fg_color)
        self.style.configure('TLabel', background=self.bg_color, foreground=self.fg_color)
        self.style.configure('TFrame', background=self.bg_color)
        self.style.configure('TButton', background=self.widget_bg, foreground=self.fg_color)
        self.style.map('TButton',
                       background=[('active', self.widget_bg), ('!active', self.widget_bg)])
        # 特殊控件
        self.style.configure('TProgressbar', background='#4caf50')
        # 输入框
        self.root.option_add('*Text*Background', self.widget_bg)
        self.root.option_add('*Text*Foreground', self.fg_color)
        self.root.option_add('*Entry*Background', self.widget_bg)
        self.root.option_add('*Entry*Foreground', self.fg_color)
        self.root.option_add('*Listbox*Background', self.widget_bg)
        self.root.option_add('*Listbox*Foreground', self.fg_color)

    def toggle_theme(self):
        self.config["theme"] = "dark" if self.config["theme"] == "light" else "light"
        self.bg_color = "#f0f0f0" if self.config["theme"] == "light" else "#2d2d2d"
        self.fg_color = "#000000" if self.config["theme"] == "light" else "#ffffff"
        self.widget_bg = "#ffffff" if self.config["theme"] == "light" else "#3c3c3c"
        self.root.configure(bg=self.bg_color)
        self.apply_theme()
        save_config(self.config)

    # ---------- 菜单栏 ----------
    def setup_menu(self):
        menubar = tk.Menu(self.root)
        # 退出菜单
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="退出", command=self.on_close)
        menubar.add_cascade(label="退出", menu=file_menu)
        # 设置菜单
        settings_menu = tk.Menu(menubar, tearoff=0)
        settings_menu.add_command(label="传输设置", command=self.open_settings)
        # settings_menu.add_command(label="切换主题", command=self.toggle_theme)
        menubar.add_cascade(label="设置", menu=settings_menu)
        # 帮助菜单
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="检查更新", command=self.check_for_updates)
        help_menu.add_command(label="关于", command=self.show_about)
        menubar.add_cascade(label="帮助", menu=help_menu)
        self.root.config(menu=menubar)

    # ---------- 界面构建 ----------
    def create_widgets(self):
        # 使用 Notebook 分页
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # --- 发送页 ---
        self.send_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.send_frame, text=" 发送文件 ")
        self.build_send_page()

        # --- 接收页 ---
        self.recv_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.recv_frame, text=" 接收文件 ")
        self.build_recv_page()

        # 状态栏
        self.status_var = tk.StringVar(value="就绪")
        self.status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def build_send_page(self):
        # 上部：目标信息
        target_frame = ttk.LabelFrame(self.send_frame, text="目标计算机")
        target_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(target_frame, text="IP 地址:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.send_ip_entry = ttk.Entry(target_frame, width=20)
        self.send_ip_entry.grid(row=0, column=1, padx=5, pady=5)
        self.send_ip_entry.insert(0, "")

        ttk.Label(target_frame, text="端口:").grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        self.send_port_entry = ttk.Entry(target_frame, width=8)
        self.send_port_entry.grid(row=0, column=3, padx=5, pady=5)
        self.send_port_entry.insert(0, str(self.config["port"]))

        # 文件选择区域
        file_sel_frame = ttk.LabelFrame(self.send_frame, text="选择文件/文件夹")
        file_sel_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        btn_frame = ttk.Frame(file_sel_frame)
        btn_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Button(btn_frame, text="添加文件", command=self.add_files).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="添加文件夹", command=self.add_folder).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="清空列表", command=self.clear_files).pack(side=tk.LEFT, padx=2)

        # 文件列表
        list_frame = ttk.Frame(file_sel_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.send_listbox = tk.Listbox(list_frame, selectmode=tk.MULTIPLE, height=6)
        send_scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.send_listbox.yview)
        self.send_listbox.config(yscrollcommand=send_scroll.set)
        self.send_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        send_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # 发送按钮
        self.send_btn = ttk.Button(self.send_frame, text="开始发送", command=self.start_send)
        self.send_btn.pack(pady=10)

        # 进度显示
        progress_frame = ttk.LabelFrame(self.send_frame, text="发送进度")
        progress_frame.pack(fill=tk.X, padx=10, pady=5)
        self.send_progress = ttk.Progressbar(progress_frame, orient=tk.HORIZONTAL, length=200, mode='determinate')
        self.send_progress.pack(fill=tk.X, padx=10, pady=5)
        self.send_info_var = tk.StringVar(value="等待发送...")
        ttk.Label(progress_frame, textvariable=self.send_info_var).pack(pady=2)

    def build_recv_page(self):
        # 本机信息
        info_frame = ttk.LabelFrame(self.recv_frame, text="本机监听信息")
        info_frame.pack(fill=tk.X, padx=10, pady=5)

        self.local_ip_var = tk.StringVar(value=get_local_ip())
        self.local_port_var = tk.StringVar(value=str(self.config["port"]))
        ttk.Label(info_frame, text="本机 IP:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Label(info_frame, textvariable=self.local_ip_var).grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Label(info_frame, text="监听端口:").grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        ttk.Label(info_frame, textvariable=self.local_port_var).grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)

        # 控制按钮
        ctrl_frame = ttk.Frame(self.recv_frame)
        ctrl_frame.pack(fill=tk.X, padx=10, pady=5)
        self.start_recv_btn = ttk.Button(ctrl_frame, text="开始监听", command=self.start_receive)
        self.start_recv_btn.pack(side=tk.LEFT, padx=5)
        self.stop_recv_btn = ttk.Button(ctrl_frame, text="停止监听", command=self.stop_receive, state=tk.DISABLED)
        self.stop_recv_btn.pack(side=tk.LEFT, padx=5)

        # 接收进度
        recv_progress_frame = ttk.LabelFrame(self.recv_frame, text="接收进度")
        recv_progress_frame.pack(fill=tk.X, padx=10, pady=5)
        self.recv_progress = ttk.Progressbar(recv_progress_frame, orient=tk.HORIZONTAL, length=200, mode='determinate')
        self.recv_progress.pack(fill=tk.X, padx=10, pady=5)
        self.recv_info_var = tk.StringVar(value="未开始监听")
        ttk.Label(recv_progress_frame, textvariable=self.recv_info_var).pack(pady=2)

        # 已接收文件列表
        recv_files_frame = ttk.LabelFrame(self.recv_frame, text="已接收文件")
        recv_files_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        list_frame = ttk.Frame(recv_files_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.recv_listbox = tk.Listbox(list_frame, height=5)
        recv_scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.recv_listbox.yview)
        self.recv_listbox.config(yscrollcommand=recv_scroll.set)
        self.recv_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        recv_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    # ---------- 文件选择操作 ----------
    def add_files(self):
        files = filedialog.askopenfilenames(title="选择文件")
        for f in files:
            if f not in self.files_to_send:
                self.files_to_send.append(f)
                self.send_listbox.insert(tk.END, f)

    def add_folder(self):
        folder = filedialog.askdirectory(title="选择文件夹")
        if folder and folder not in self.files_to_send:
            self.files_to_send.append(folder)
            self.send_listbox.insert(tk.END, folder + " (文件夹)")

    def clear_files(self):
        self.files_to_send.clear()
        self.send_listbox.delete(0, tk.END)

    # ---------- 打包发送 ----------
    def get_packaged_file(self):
        """将待发送列表打包成单个临时文件（如果仅一个文件则直接返回路径）"""
        if not self.files_to_send:
            return None, None
        if len(self.files_to_send) == 1 and os.path.isfile(self.files_to_send[0]):
            return self.files_to_send[0], False  # 单文件，不压缩
        # 多个文件或包含文件夹，打包为zip
        tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        tmp_path = tmp.name
        tmp.close()
        with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for item in self.files_to_send:
                if os.path.isfile(item):
                    zf.write(item, os.path.basename(item))
                elif os.path.isdir(item):
                    for root, dirs, files in os.walk(item):
                        for file in files:
                            full_path = os.path.join(root, file)
                            arcname = os.path.relpath(full_path, os.path.dirname(item))
                            zf.write(full_path, arcname)
        return tmp_path, True  # 返回临时zip路径及是否为打包

    # ---------- 发送线程启动 ----------
    def start_send(self):
        if not self.files_to_send:
            messagebox.showwarning("提示", "请先添加要发送的文件或文件夹")
            return
        host = self.send_ip_entry.get().strip()
        if not host:
            messagebox.showwarning("提示", "请输入目标 IP 地址")
            return
        try:
            port = int(self.send_port_entry.get().strip())
        except ValueError:
            messagebox.showwarning("提示", "端口必须是整数")
            return

        filepath, is_pack = self.get_packaged_file()
        if not filepath or not os.path.exists(filepath):
            messagebox.showwarning("提示", "文件不存在或打包失败")
            return

        self.send_btn.config(state=tk.DISABLED)
        self.status_var.set("正在发送...")
        # 选择传输类
        mode = self.config["mode"]
        transfer_cls = TCPTransfer if mode == "tcp" else P2PTransfer
        thread = threading.Thread(target=self._send_worker, args=(transfer_cls, host, port, filepath, is_pack), daemon=True)
        thread.start()

    def _send_worker(self, transfer_cls, host, port, filepath, is_pack):
        try:
            transfer_cls.send_file(host, port, filepath, self.progress_queue)
        except Exception as e:
            self.progress_queue.put({"type": "error", "msg": f"发送失败: {e}"})
        finally:
            # 清理临时打包文件
            if is_pack and os.path.exists(filepath):
                os.unlink(filepath)
            self.progress_queue.put({"type": "enable_send_btn"})

    # ---------- 接收线程 ----------
    def start_receive(self):
        # 如果已有线程正在运行，先停止再启动（确保端口释放）
        if self.recv_thread and self.recv_thread.is_alive():
            self.stop_receive()
            # 等待旧线程结束（最多等待2秒）
            self.recv_thread.join(timeout=2.0)
            self.recv_thread = None

        port = self.config["port"]
        save_dir = self.config["download_dir"]
        os.makedirs(save_dir, exist_ok=True)

        self.recv_stop_event.clear()
        mode = self.config["mode"]
        transfer_cls = TCPTransfer if mode == "tcp" else P2PTransfer

        # 启动接收工作线程
        self.recv_thread = threading.Thread(target=self._recv_worker, args=(transfer_cls, port, save_dir), daemon=True)
        self.recv_thread.start()

        self.start_recv_btn.config(state=tk.DISABLED)
        self.stop_recv_btn.config(state=tk.NORMAL)
        self.local_port_var.set(str(port))
        self.status_var.set(f"监听中... ({mode.upper()} 端口 {port})")

    def _recv_worker(self, transfer_cls, port, save_dir):
        socket_holder = []  # 用于捕获服务器 socket
        try:
            transfer_cls.start_server(port, save_dir, self.progress_queue,
                                      stop_event=self.recv_stop_event,
                                      socket_holder=socket_holder)
        except Exception as e:
            self.progress_queue.put({"type": "error", "msg": f"接收服务异常: {e}"})
        finally:
            # 线程结束，清理引用
            self.recv_socket = None
            self.progress_queue.put({"type": "recv_stopped"})
            if socket_holder:
                self.recv_socket = socket_holder[0]  # 实际上线程已结束，socket已关闭，这里仅记录

    def stop_receive(self):
        self.recv_stop_event.set()  # 设置停止标志
        if self.recv_socket:
            try:
                self.recv_socket.close()  # 强制关闭 socket，使阻塞的 accept/recvfrom 立即返回
            except:
                pass
            self.recv_socket = None
        self.start_recv_btn.config(state=tk.NORMAL)
        self.stop_recv_btn.config(state=tk.DISABLED)
        self.status_var.set("监听已停止")

    # ---------- UI 更新循环 ----------
    def update_progress_loop(self):
        try:
            while True:
                msg = self.progress_queue.get_nowait()
                self.process_message(msg)
        except queue.Empty:
            pass
        self.root.after(100, self.update_progress_loop)

    def process_message(self, msg):
        t = msg.get("type")
        if t == "progress":
            filename = msg.get("filename", "")
            percent = msg.get("percent", 0)
            speed = msg.get("speed", 0)
            size = msg.get("size", 0)
            sent = msg.get("sent", 0)
            # 更新对应的进度条和信息（发送页或接收页根据上下文？这里简化：全部更新到两个页）
            info_text = f"{filename} - {sent}/{size} 字节 ({percent:.1f}%)  速率: {self._format_speed(speed)}"
            self.send_info_var.set(info_text)
            self.recv_info_var.set(info_text)
            self.send_progress['value'] = percent
            self.recv_progress['value'] = percent
        elif t == "done":
            filename = msg.get("filename", "")
            self.status_var.set(f"传输完成: {filename}")
            self.send_info_var.set("传输完成")
            self.recv_info_var.set("传输完成")
            self.send_progress['value'] = 100
            self.recv_progress['value'] = 100
        elif t == "error":
            messagebox.showerror("错误", msg.get("msg", ""))
            self.status_var.set("出错")
        elif t == "status":
            self.status_var.set(msg.get("msg", ""))
        elif t == "enable_send_btn":
            self.send_btn.config(state=tk.NORMAL)
        elif t == "recv_stopped":
            self.start_recv_btn.config(state=tk.NORMAL)
            self.stop_recv_btn.config(state=tk.DISABLED)

    @staticmethod
    def _format_speed(speed):
        """将字节/秒转为可读字符串"""
        if speed < 1024:
            return f"{speed:.1f} B/s"
        elif speed < 1024*1024:
            return f"{speed/1024:.1f} KB/s"
        else:
            return f"{speed/(1024*1024):.1f} MB/s"

    # ---------- 设置窗口 ----------
    def open_settings(self):
        win = Toplevel(self.root)
        win.title("传输设置")
        win.geometry("400x300")
        win.transient(self.root)
        win.grab_set()

        frame = ttk.Frame(win, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="传输模式:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        mode_var = tk.StringVar(value=self.config["mode"])
        mode_combo = ttk.Combobox(frame, textvariable=mode_var, values=["tcp", "UDP"], state="readonly", width=10)
        mode_combo.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(frame, text="主题:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        theme_var = tk.StringVar(value=self.config["theme"])
        theme_combo = ttk.Combobox(frame, textvariable=theme_var, values=["light"], state="readonly", width=10)
        theme_combo.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(frame, text="默认端口:").grid(row=2, column=0, padx=5, pady=5, sticky=tk.W)
        port_var = tk.IntVar(value=self.config["port"])
        port_entry = ttk.Entry(frame, textvariable=port_var, width=10)
        port_entry.grid(row=2, column=1, padx=5, pady=5, sticky=tk.W)

        ttk.Label(frame, text="下载目录:").grid(row=3, column=0, padx=5, pady=5, sticky=tk.W)
        dir_var = tk.StringVar(value=self.config["download_dir"])
        dir_entry = ttk.Entry(frame, textvariable=dir_var, width=30)
        dir_entry.grid(row=3, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Button(frame, text="浏览", command=lambda: dir_var.set(filedialog.askdirectory())).grid(row=3, column=2, padx=5)

        def save_settings():
            new_mode = mode_var.get()
            new_theme = theme_var.get()
            new_port = port_var.get()
            new_dir = dir_var.get()

            # 检查是否需要重启接收服务（如果模式或端口发生变化且接收正在运行）
            restart_recv = False
            if self.recv_thread and self.recv_thread.is_alive():
                if (new_mode != self.config["mode"] or new_port != self.config["port"]):
                    restart_recv = True

            # 更新配置
            self.config["mode"] = new_mode
            self.config["theme"] = new_theme
            self.config["port"] = new_port
            self.config["download_dir"] = new_dir
            save_config(self.config)

            # 主题立即应用
            if self.config["theme"] != new_theme:
                self.config["theme"] = new_theme
                self.bg_color = "#f0f0f0" if new_theme == "light" else "#2d2d2d"
                self.fg_color = "#000000" if new_theme == "light" else "#ffffff"
                self.widget_bg = "#ffffff" if new_theme == "light" else "#3c3c3c"
                self.root.configure(bg=self.bg_color)
                self.apply_theme()

            # 更新 UI 上的端口显示
            self.local_port_var.set(str(new_port))
            self.send_port_entry.delete(0, tk.END)
            self.send_port_entry.insert(0, str(new_port))

            # 如果需要重启接收服务
            if restart_recv:
                self.stop_receive()
                # 等待旧的接收线程退出
                if self.recv_thread and self.recv_thread.is_alive():
                    self.recv_thread.join(timeout=2.0)
                # 重新启动接收
                self.start_receive()

            win.destroy()
            messagebox.showinfo("提示", "设置已保存")

        ttk.Button(frame, text="保存", command=save_settings).grid(row=4, column=1, pady=15)

    # ---------- 检查更新 ----------
    def check_for_updates(self):
        repo = self.config.get("github_repo", "")
        if "/" not in repo or repo.startswith("your-"):
            messagebox.showinfo("检查更新", "请先在设置中配置有效的 GitHub 仓库地址（格式: 用户名/仓库名）")
            return
        url = f"https://api.github.com/repos/{repo}/releases/latest"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "LANFileTransfer"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                latest_tag = data.get("tag_name", "")
                if latest_tag:
                    if latest_tag != VERSION:
                        if messagebox.askyesno("发现新版本", f"最新版本: {latest_tag}\n当前版本: {VERSION}\n是否打开下载页面？"):
                            import webbrowser
                            webbrowser.open(data.get("html_url", f"https://github.com/{repo}/releases"))
                    else:
                        messagebox.showinfo("检查更新", "已是最新版本")
                else:
                    raise ValueError("No tag")
        except Exception as e:
            messagebox.showerror("错误", f"无法获取更新信息: {e}")

    def show_about(self):
        messagebox.showinfo("关于", f"局域网文件传输工具\n版本 {VERSION}\n作者: CUMDER\nGitHub: {self.config['github_repo']}")

    def on_close(self):
        self.stop_receive()  # 停止监听
        self.recv_stop_event.set()
        self.root.destroy()

#  ================== 入口 ==================
if __name__ == "__main__":
    root = tk.Tk()
    app = LANFileTransferApp(root)
    root.mainloop()