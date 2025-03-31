import os
from music21 import stream, note, chord, instrument, tempo, meter
from utils import convert_midi_to_wav


def create_blues_music(output_dir):
    """生成一段20秒的布鲁斯音乐，包含电吉他、贝斯、鼓、口琴和钢琴"""
    os.makedirs(output_dir, exist_ok=True)

    # 设置布鲁斯的基础节奏和速度
    bpm = 90  # 每分钟90拍
    blues_progression = [
        ('C4', 'E4', 'G4'),  # C和弦
        ('F3', 'A3', 'C4'),  # F和弦
        ('G3', 'B3', 'D4'),  # G和弦
    ]

    # 创建乐器声部
    parts = {
        "electric_guitar": stream.Part(),
        "bass": stream.Part(),
        "drums": stream.Part(),
        "harmonica": stream.Part(),
        "piano": stream.Part(),
    }

    # 设置乐器
    parts["electric_guitar"].append(instrument.ElectricGuitar())
    parts["bass"].append(instrument.ElectricBass())
    parts["drums"].append(instrument.SnareDrum())
    parts["harmonica"].append(instrument.Harmonica())
    parts["piano"].append(instrument.Piano())

    # 设置速度和节拍
    for part in parts.values():
        part.append(tempo.MetronomeMark(number=bpm))
        part.append(meter.TimeSignature('4/4'))

    # 电吉他声部
    for chord_notes in blues_progression:
        c = chord.Chord(chord_notes)
        c.quarterLength = 4
        parts["electric_guitar"].append(c)

    # 贝斯声部
    for root_note in ['C2', 'F2', 'G2']:
        for _ in range(4):  # 每个和弦分4拍
            n = note.Note(root_note)
            n.quarterLength = 1
            parts["bass"].append(n)

# 鼓声部
    for _ in range(12):  # 12小节布鲁斯
        n = note.Unpitched()
        n.displayName = 'Snare Drum'  # 设置显示名称为 Snare Drum
        n.quarterLength = 1
        parts["drums"].append(n)

    # 口琴声部
    for chord_notes in blues_progression:
        c = chord.Chord(chord_notes)
        c.quarterLength = 4
        parts["harmonica"].append(c)

    # 钢琴声部
    for chord_notes in blues_progression:
        c = chord.Chord(chord_notes)
        c.quarterLength = 4
        parts["piano"].append(c)

    # 输出每个乐器的 MIDI 文件
    for name, part in parts.items():
        midi_file = os.path.join(output_dir, f"{name}.mid")
        part.write('midi', fp=midi_file)
        print(f"{name} MIDI 文件已生成: {midi_file}")

    # 合并所有声部
    full_score = stream.Score()
    for part in parts.values():
        full_score.append(part)

    # 输出合并的 MIDI 文件
    combined_midi_file = os.path.join(output_dir, "blues_combined.mid")
    full_score.write('midi', fp=combined_midi_file)
    print(f"合并的 MIDI 文件已生成: {combined_midi_file}")

    # 转换为音频文件
    convert_midi_to_wav(output_dir)
    print(f"音频文件已生成: {os.path.join(output_dir, 'blues_combined.wav')}")


def create_electric_guitar_part(output_dir):
    """生成更丰富的电吉他声部布鲁斯音乐"""
    # 创建电吉他声部
    electric_guitar_part = stream.Part()
    electric_guitar_part.append(instrument.ElectricGuitar())
    electric_guitar_part.append(tempo.MetronomeMark(number=90))  # 设置速度为每分钟90拍
    electric_guitar_part.append(meter.TimeSignature('4/4'))  # 设置节拍为4/4

    # 12小节布鲁斯和弦进行
    blues_progression = [
        ('C4', 'E4', 'G4'),  # C和弦
        ('F3', 'A3', 'C4'),  # F和弦
        ('G3', 'B3', 'D4'),  # G和弦
    ]

    # 填充12小节布鲁斯，加入分解和弦和旋律
    for _ in range(4):  # 4次循环，每次3个和弦，共12小节
        for chord_notes in blues_progression:
            # 添加和弦扫弦
            c = chord.Chord(chord_notes)
            c.quarterLength = 2  # 和弦持续2拍
            electric_guitar_part.append(c)

            # 添加分解和弦
            for pitch in chord_notes:
                n = note.Note(pitch)
                n.quarterLength = 0.5  # 每个音符持续半拍
                electric_guitar_part.append(n)

            # 添加单音旋律
            melody = ['G4', 'A4', 'C5', 'D5']  # 即兴旋律
            for pitch in melody:
                n = note.Note(pitch)
                n.quarterLength = 0.5
                n.volume.velocity = 90  # 设置力度
                electric_guitar_part.append(n)

    # 添加结束段落
    ending_chord = chord.Chord(['C4', 'E4', 'G4'])
    ending_chord.quarterLength = 4  # 延长和弦
    electric_guitar_part.append(ending_chord)

    # 输出电吉他声部的 MIDI 文件
    midi_file = f"{output_dir}/electric_guitar.mid"
    electric_guitar_part.write('midi', fp=midi_file)
    print(f"电吉他声部 MIDI 文件已生成: {midi_file}")
    # 转换为音频文件
    convert_midi_to_wav(output_dir)
    print(f"音频文件已生成: {os.path.join(output_dir, 'electric_guitar.wav')}")


if __name__ == "__main__":
    create_electric_guitar_part("output/blues_music")
