import os
from datetime import datetime
from midi2audio import FluidSynth


PROJECT_DIR = "output"


def create_project_dir(postfix=""):
    # Create project root dir
    # project_name = "_".join(
    #     [datetime.now().strftime("%Y-%m-%d"),
    #     base64.b64encode(os.urandom(4)).decode('utf-8')[:6]])
    project_name = f"{datetime.now().strftime('%Y-%m-%d')}"
    project_name = f"music_{project_name}_{postfix}"

    output_dir = os.path.join(PROJECT_DIR, project_name)  # hard code
    os.makedirs(output_dir, exist_ok=True)
    # Create midi dir
    midi_dir = os.path.join(output_dir, "midi")
    os.makedirs(midi_dir, exist_ok=True)
    # Create wav dir
    wav_dir = os.path.join(output_dir, "wav")
    os.makedirs(wav_dir, exist_ok=True)
    return output_dir


def convert_midi_to_wav(midi_dir, soundfont_path=None):
    """将MIDI文件转换为WAV文件"""
    if soundfont_path is None:
        soundfont_path = find_soundfont_path()
    # 创建 FluidSynth 实例
    fs = FluidSynth(sound_font=soundfont_path)

    # 转换所有 MIDI 文件
    for filename in os.listdir(midi_dir):
        if filename.endswith('.mid'):
            midi_file = os.path.join(midi_dir, filename)
            wav_file = os.path.join(midi_dir, filename.replace('.mid', '.wav'))

            # 检查 WAV 文件是否已存在
            if os.path.exists(wav_file):
                print(f"音频文件已存在：{wav_file}, 准备覆盖")
                os.remove(wav_file)

            print(f"正在转换 {filename}...")
            fs.midi_to_audio(midi_file, wav_file)
            print(f"已生成：{wav_file}")


def find_soundfont_path():
    soundfont_path = None
    """查找音色库文件路径"""
    possible_paths = [
        '/opt/homebrew/share/sounds/sf2/FluidR3_GM.sf2',
        '/usr/share/sounds/sf2/FluidR3_GM.sf2',
        'soundfonts/FluidR3_GM.sf2'
    ]
    for path in possible_paths:
        if os.path.exists(path):
            soundfont_path = path
            break

    if soundfont_path is None:
        raise FileNotFoundError("找不到音色库文件，请安装 FluidSynth 并指定音色库路径")
    return soundfont_path
