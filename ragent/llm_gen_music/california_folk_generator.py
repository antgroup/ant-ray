import os
from datetime import datetime
from music21 import stream, note, instrument, meter, tempo, expressions, chord, articulations, spanner, volume
from utils import create_project_dir, convert_midi_to_wav


def create_folk_guitar_solo(output_dir):
    """生成加州风格的民谣古典原声吉他独奏"""
    guitar_part = stream.Part()

    # 使用原声吉他音色
    guitar_part.append(instrument.AcousticGuitar())
    guitar_part.append(tempo.MetronomeMark(number=120))  # 轻快的节奏 (BPM=120)

    # 定义吉他独奏段落
    solo = [
        # 开场：扫弦
        (('C4', 'E4', 'G4'), 1.0, 'mf', 'arpeggio'),  # C和弦扫弦
        (('G3', 'B3', 'D4'), 1.0, 'mf', 'arpeggio'),  # G和弦扫弦
        (('A3', 'C4', 'E4'), 1.0, 'mf', 'arpeggio'),  # Am和弦扫弦
        (('F3', 'A3', 'C4'), 1.0, 'mf', 'arpeggio'),  # F和弦扫弦

        # 中段：旋律结合和弦
        ('E4', 0.5, 'mf', 'normal'),
        ('G4', 0.5, 'mf', 'normal'),
        ('A4', 0.5, 'mf', 'normal'),
        ('C5', 0.5, 'mf', 'normal'),
        (('D4', 'F4', 'A4'), 1.0, 'mf', 'arpeggio'),  # Dm和弦扫弦

        # 快速分解和弦
        (('C4', 'E4', 'G4'), 0.5, 'mf', 'arpeggio'),
        (('G3', 'B3', 'D4'), 0.5, 'mf', 'arpeggio'),
        (('A3', 'C4', 'E4'), 0.5, 'mf', 'arpeggio'),
        (('F3', 'A3', 'C4'), 0.5, 'mf', 'arpeggio'),

        # 结束段落：延长音和颤音
        (('C4', 'E4', 'G4'), 2.0, 'mf', 'arpeggio'),  # 延长和弦
        ('G4', 2.0, 'mf', 'trill'),                  # G音颤音
        (('G3', 'B3', 'D4'), 2.0, 'mf', 'arpeggio'),  # G和弦扫弦
        # 新增部分：快速分解和弦和旋律结合
        (('E4', 'G4', 'C5'), 0.5, 'mf', 'arpeggio'),  # C和弦高音分解
        (('D4', 'F4', 'A4'), 0.5, 'mf', 'arpeggio'),  # Dm和弦分解
        ('E4', 0.5, 'mf', 'normal'),                 # 单音旋律
        ('G4', 0.5, 'mf', 'normal'),
        ('A4', 0.5, 'mf', 'normal'),
        ('C5', 0.5, 'mf', 'normal'),

        # 新增部分：延长和弦和颤音
        (('F3', 'A3', 'C4'), 2.0, 'mf', 'arpeggio'),  # F和弦延长
        ('A4', 2.0, 'mf', 'trill'),                  # A音颤音
        (('E3', 'G3', 'C4'), 2.0, 'mf', 'arpeggio'),  # C和弦延长

        # 新增部分：快速上行分解和弦
        (('C4', 'E4', 'G4'), 0.5, 'mf', 'arpeggio'),
        (('D4', 'F4', 'A4'), 0.5, 'mf', 'arpeggio'),
        (('E4', 'G4', 'B4'), 0.5, 'mf', 'arpeggio'),
        (('F4', 'A4', 'C5'), 0.5, 'mf', 'arpeggio'),

        # 新增部分：结束段落
        (('C4', 'E4', 'G4'), 2.0, 'mf', 'arpeggio'),  # C和弦延长
        ('G4', 2.0, 'mf', 'trill'),                  # G音颤音
        (('G3', 'B3', 'D4'), 2.0, 'mf', 'arpeggio'),  # G和弦延长
        # 新增部分：快速分解和弦和旋律结合
        (('A3', 'C4', 'E4'), 0.5, 'mf', 'arpeggio'),  # Am和弦分解
        (('G3', 'B3', 'D4'), 0.5, 'mf', 'arpeggio'),  # G和弦分解
        ('E4', 0.5, 'mf', 'normal'),                 # 单音旋律
        ('F4', 0.5, 'mf', 'normal'),
        ('G4', 0.5, 'mf', 'normal'),
        ('A4', 0.5, 'mf', 'normal'),

        # 新增部分：延长和弦和颤音
        (('D4', 'F4', 'A4'), 2.0, 'mf', 'arpeggio'),  # Dm和弦延长
        ('F4', 2.0, 'mf', 'trill'),                  # F音颤音
        (('C4', 'E4', 'G4'), 2.0, 'mf', 'arpeggio'),  # C和弦延长

        # 新增部分：快速下行分解和弦
        (('F4', 'A4', 'C5'), 0.5, 'mf', 'arpeggio'),
        (('E4', 'G4', 'B4'), 0.5, 'mf', 'arpeggio'),
        (('D4', 'F4', 'A4'), 0.5, 'mf', 'arpeggio'),
        (('C4', 'E4', 'G4'), 0.5, 'mf', 'arpeggio'),

        # 新增部分：结束段落
        (('A3', 'C4', 'E4'), 2.0, 'mf', 'arpeggio'),  # Am和弦延长
        ('E4', 2.0, 'mf', 'trill'),                  # E音颤音
        (('F3', 'A3', 'C4'), 2.0, 'mf', 'arpeggio'),  # F和弦延长
    ]

    # 将独奏片段添加到音轨中
    for pitches, duration, dynamic, effect in solo:
        if isinstance(pitches, tuple):
            n = chord.Chord(pitches)
        else:
            n = note.Note(pitches)

        n.duration.quarterLength = duration
        n.volume.velocity = get_velocity(dynamic)

        # 添加表现力效果
        if effect == 'arpeggio':
            n.articulations = [articulations.Tenuto()]  # 使用 Tenuto 表示柔和的扫弦效果
        elif effect == 'trill':
            n.expressions = [expressions.Trill()]  # 颤音效果
        elif effect == 'normal':
            n.articulations = []

        guitar_part.append(n)

    os.makedirs(output_dir, exist_ok=True)
    midi_file = os.path.join(output_dir, 'folk_guitar_solo.mid')
    guitar_part.write('midi', fp=midi_file)
    print(f"民谣吉他独奏MIDI文件已生成: {midi_file}")
    return guitar_part


def get_velocity(dynamic):
    """将动态标记映射到MIDI力度值"""
    mapping = {
        'pp': 40,
        'p': 55,
        'mp': 70,
        'mf': 85,
        'f': 100,
        'ff': 115,
        'fff': 127
    }
    return mapping.get(dynamic, 85)


if __name__ == "__main__":
    create_folk_guitar_solo("output/music_folk")
    convert_midi_to_wav("output/music_folk")