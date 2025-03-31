import os
from datetime import datetime
from music21 import stream, note, instrument, meter, tempo, expressions, chord, articulations, spanner
from utils import create_project_dir, convert_midi_to_wav


def create_simple_melody(instrument_name='Piano'):
    """创建一个简单的旋律"""
    # 创建一个新的音乐流
    s = stream.Stream()
    
    # 设置乐器
    if instrument_name == 'Piano':
        instrument_obj = instrument.Piano()
    elif instrument_name == 'Violin':
        instrument_obj = instrument.Violin()
    elif instrument_name == 'Flute':
        instrument_obj = instrument.Flute()
    elif instrument_name == 'Guitar':
        instrument_obj = instrument.Guitar()
    else:
        instrument_obj = instrument.Piano()
    
    # 将乐器添加到流中
    s.append(instrument_obj)
    
    # 添加一些音符
    notes = [
        ('C4', 1.0),  # 音符和持续时间（秒）
        ('E4', 1.0),
        ('G4', 1.0),
        ('C5', 1.0),
        ('G4', 1.0),
        ('E4', 1.0),
        ('C4', 1.0),
        ('G3', 1.0),
        ('C4', 1.0)
    ]
    
    # 添加音符到流中
    for note_name, duration in notes:
        n = note.Note(note_name)
        n.duration.quarterLength = duration
        s.append(n)
    
    return s

def create_hotel_california_melody():
    """创建一个原创的吉他旋律"""
    s = stream.Stream()
    
    # 设置吉他作为乐器
    s.append(instrument.Guitar())
    
    # 创建一个原创的旋律
    # 使用 Em 和 Am 调式，类似民谣吉他风格
    notes = [
        # 第一小节
        ('E4', 0.5), ('B3', 0.5), ('G3', 0.5), ('B3', 0.5),
        ('E4', 0.5), ('B3', 0.5), ('G3', 0.5), ('B3', 0.5),
        
        # 第二小节
        ('A3', 0.5), ('C4', 0.5), ('E4', 0.5), ('C4', 0.5),
        ('A3', 0.5), ('C4', 0.5), ('E4', 0.5), ('C4', 0.5),
        
        # 第三小节 - 变奏
        ('G3', 0.5), ('B3', 0.5), ('D4', 0.5), ('B3', 0.5),
        ('G3', 0.5), ('B3', 0.5), ('D4', 0.5), ('B3', 0.5),
        
        # 第四小节 - 结束句
        ('E4', 1.0), ('B3', 1.0), 
        ('G3', 1.0), ('E3', 1.0)
    ]
    
    # 添加音符到流中
    for note_name, duration in notes:
        n = note.Note(note_name)
        n.duration.quarterLength = duration
        s.append(n)
    
    return s

def create_classical_guitar():
    """创建古典吉他声部，添加音色和表现力参数"""
    guitar_part = stream.Part()
    # 使用尼龙弦吉他音色
    guitar_part.append(instrument.Guitar())
    
    # 设置基础速度和表情
    guitar_part.append(tempo.MetronomeMark(number=92))
    
    # 创建主旋律，包含更多表现力参数
    riff = [
        # 第一段：温柔的引入 (p - piano)
        ('E4', 0.5, 'p', 'normal'),
        ('G4', 0.5, 'mp', 'harmonic'),  # 使用泛音
        ('A4', 0.45, 'p', 'normal'),
        ('E4', 0.55, 'mp', 'tremolo'),  # 添加颤音效果
        
        # 第二段：渐强 (crescendo)
        ('G4', 0.5, 'mf', 'normal'),
        ('A4', 0.5, 'f', 'trill'),   # 添加揉弦效果
        ('B4', 1.1, 'f', 'tenuto'),    # 持续音
    ]
    
    # 添加表现力
    for pitch, duration, dynamic, effect in riff:
        n = note.Note(pitch)
        n.duration.quarterLength = duration
        
        # 添加力度
        n.volume.velocity = get_velocity(dynamic)
        
        # 添加效果
        if effect == 'harmonic':
            n.harmonicType = 'natural'
        elif effect == 'tremolo':
            n.expressions = [expressions.Tremolo()]
        elif effect == 'trill':
            n.expressions = [expressions.Trill()]
        elif effect == 'tenuto':
            n.articulations = [articulations.Tenuto()]
        
        guitar_part.append(n)
            
    return guitar_part

def create_piano():
    """创建钢琴声部，添加丰富的表现力"""
    piano_part = stream.Part()
    piano_part.append(instrument.Piano())
    
    # 创建一个主题动机
    motif = [
        # 使用和弦和单音结合
        (('C4', 'E4', 'G4'), 2.0, 'mf', 'chord'),    # 和弦
        ('A4', 0.75, 'mp', 'staccato'),              # 断音
        ('G4', 0.25, 'p', 'tenuto'),                 # 持续音
        ('F4', 1.0, 'mf', 'trill'),                  # 加入装饰音
    ]
    
    for pitches, duration, dynamic, articulation in motif:
        if isinstance(pitches, tuple):
            # 创建和弦
            c = chord.Chord(pitches)
            c.duration.quarterLength = duration
            c.volume.velocity = get_velocity(dynamic)
            piano_part.append(c)
        else:
            # 创建单音符
            n = note.Note(pitches)
            n.duration.quarterLength = duration
            n.volume.velocity = get_velocity(dynamic)
            
            # 添加演奏方式
            if articulation == 'staccato':
                n.articulations = [articulations.Staccato()]
            elif articulation == 'tenuto':
                n.articulations = [articulations.Tenuto()]
            elif articulation == 'trill':
                n.expressions = [expressions.Trill()]
                
            piano_part.append(n)
    
    return piano_part

def create_bass():
    """创建贝斯声部"""
    bass_part = stream.Part()
    bass_part.append(instrument.ElectricBass())
    bass_part.append(meter.TimeSignature('4/4'))
    
    # 贝斯从开始就进入，但音量较小
    bass_line = [
        ('E2', 0.95, 'mp'), ('E2', 1.05, 'mp'), ('A2', 0.98, 'mp'), ('G2', 1.02, 'mp'),
        ('E2', 1.0, 'mf'), ('E2', 1.0, 'mf'), ('D2', 0.95, 'mp'), ('E2', 1.05, 'mp'),
    ]
    
    # 重复4次，逐渐增强后减弱
    dynamics = ['mp', 'mf', 'f', 'mp']
    for i in range(4):
        base_dynamic = dynamics[i]
        for pitch, duration, dynamic in bass_line:
            n = note.Note(pitch)
            n.duration.quarterLength = duration
            n.volume.velocity = get_velocity(base_dynamic, dynamic)
            bass_part.append(n)
    
    return bass_part

def create_drums():
    """创建鼓组声部，添加复杂节奏和表现力"""
    drum_part = stream.Part()
    drum_part.append(instrument.Percussion())
    
    # 设置基础速度，带有渐快效果
    drum_part.append(tempo.MetronomeMark(number=92))
    drum_part.append(tempo.TempoText("Accelerando"))
    
    # 创建一个带摇摆感的节奏型
    groove = [
        # 基本节奏型，包含切分音
        (35, 1.0, 'mf', 'normal'),      # 底鼓
        (42, 0.33, 'mp', 'swing'),      # 踩镲（摇摆节奏）
        (38, 0.67, 'f', 'accent'),      # 军鼓（重音）
        (42, 0.5, 'p', 'normal'),       # 踩镲
        
        # 变奏部分
        (35, 0.75, 'f', 'normal'),      # 底鼓（切分）
        (38, 0.25, 'ff', 'accent'),     # 军鼓（强调）
        (49, 0.5, 'mf', 'crash'),       # 镲片
    ]
    
    for pitch, duration, dynamic, style in groove:
        n = note.Note(pitch)
        n.duration.quarterLength = duration
        n.volume.velocity = get_velocity(dynamic)
        
        # 添加演奏风格
        if style == 'accent':
            n.articulations = [articulations.Accent()]
        elif style == 'swing':
            n.duration.quarterLength *= 1.5  # 延长时值以创造摇摆感
            
        drum_part.append(n)
    
    return drum_part

def get_velocity(base_dynamic, note_dynamic='mf'):
    """更详细的力度映射"""
    return {
        'ppp': 20,   # 最弱
        'pp': 40,    # 很弱
        'p': 55,     # 弱
        'mp': 70,    # 中弱
        'mf': 85,    # 中强
        'f': 100,    # 强
        'ff': 115,   # 很强
        'fff': 127   # 最强
    }.get(note_dynamic, 85)

def generate_rock_band(output_dir='output'):
    """生成摇滚乐队的MIDI文件"""
    os.makedirs(output_dir, exist_ok=True)
    
    # 创建总谱
    score = stream.Score()
    
    # 创建各个声部
    guitar_part = create_classical_guitar()
    bass_part = create_bass()
    drum_part = create_drums()
    
    # 设置声部名称和整体音量
    guitar_part.partName = 'Classical Guitar'
    bass_part.partName = 'Electric Bass'
    drum_part.partName = 'Drums'
    
    # 将所有声部添加到总谱中
    score.append([guitar_part, bass_part, drum_part])
    
    # 保存总谱为MIDI文件
    midi_file = os.path.join(output_dir, 'rock_band.mid')
    score.write('midi', fp=midi_file)
    print(f"摇滚乐队MIDI文件已生成: {midi_file}")
    
    # 同时保存各个声部的独立MIDI文件（用于单独查看/编辑）
    parts = {
        'guitar': guitar_part,
        'bass': bass_part,
        'drums': drum_part
    }
    
    for name, part in parts.items():
        part_file = os.path.join(output_dir, f'rock_{name}.mid')
        part.write('midi', fp=part_file)
        print(f"{name} 声部MIDI文件已生成: {part_file}")


if __name__ == "__main__":
    generate_rock_band("output/music_rock_band4")
    convert_midi_to_wav("output/music_rock_band4") 