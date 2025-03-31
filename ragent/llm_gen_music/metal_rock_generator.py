import os
from music21 import stream, note, instrument, tempo, expressions, chord, articulations, spanner, volume
from utils import convert_midi_to_wav


def create_metal_lead_guitar(output_dir):
    """创建金属摇滚主音吉他声部"""
    guitar_part = stream.Part()
    
    # 使用失真电吉他音色
    guitar_part.append(instrument.ElectricGuitar())
    
    # 设置快速的基础速度 (BPM=150)
    guitar_part.append(tempo.MetronomeMark(number=150))
    
    # 创建金属风格的主旋律
    riff = [
        # 开场 Power Chord 力量和弦
        (('E4', 'B4'), 1.0, 'ff', 'accent'),    # 延长和弦时值
        (('E4', 'B4'), 1.0, 'ff', 'accent'),
        
        # 主旋律段落（调整时值）
        ('E5', 0.5, 'f', 'slide'),      
        ('F5', 0.5, 'f', 'bend'),       
        ('G5', 0.5, 'f', 'trill'),      
        ('A5', 0.5, 'f', 'normal'),
        
        # 技巧段落
        ('B5', 1.0, 'ff', 'tremolo'),    
        ('A5', 0.5, 'f', 'bend'),       
        ('G5', 0.5, 'f', 'normal'),
        ('E5', 1.0, 'ff', 'slide'),     
        
        # 快速下行段落
        ('E5', 0.25, 'f', 'normal'),
        ('D5', 0.25, 'f', 'normal'),
        ('C5', 0.25, 'f', 'normal'),
        ('B4', 0.25, 'f', 'tremolo'),
        
        # 结束和弦
        (('E4', 'B4'), 2.0, 'ff', 'accent'),
    ]
    
    # 增加重复次数以达到30秒
    for _ in range(6):
        for pitches, duration, dynamic, effect in riff:
            # 处理和弦和单音
            if isinstance(pitches, tuple):
                n = chord.Chord(pitches)
            else:
                n = note.Note(pitches)
            
            # 设置时值
            n.duration.quarterLength = duration
            
            # 设置力度
            n.volume.velocity = get_velocity(dynamic)
            
            # 添加表现力效果
            if effect == 'accent':
                n.articulations = [articulations.Accent()]
            elif effect == 'slide':
                n.expressions = [expressions.Mordent()]
            elif effect == 'bend':
                # 简单使用较高的力度来模拟弯音效果
                n.volume = volume.Volume(velocity=110)
            elif effect == 'tremolo':
                n.expressions = [expressions.Tremolo()]
            elif effect == 'trill':
                n.expressions = [expressions.Trill()]
            
            guitar_part.append(n)

    os.makedirs(output_dir, exist_ok=True)
    midi_file = os.path.join(output_dir, 'metal_lead_guitar.mid')
    guitar_part.write('midi', fp=midi_file)
    print(f"金属吉他MIDI文件已生成: {midi_file}")
    return guitar_part


def get_velocity(dynamic):
    """更详细的力度映射，适合金属风格"""
    return {
        'mf': 90,    # 中强
        'f': 105,    # 强
        'ff': 120,   # 很强
        'fff': 127   # 最强
    }.get(dynamic, 105)  # 默认为强


def create_metal_drums(output_dir):
    """创建金属摇滚架子鼓声部"""
    drum_part = stream.Part()
    
    # 使用 TomTom 作为基础打击乐器
    drum_part.append(instrument.TomTom())
    drum_part.append(tempo.MetronomeMark(number=150))
    
    # 定义鼓组音符（使用标准 GM 打击乐器映射）
    beat_pattern = [
        # 第一小节：开场
        (49, 1.0, 'fff'),    # Crash cymbal
        (36, 0.5, 'ff'),     # Bass drum
        (38, 0.5, 'f'),      # Snare drum
        
        # 第二小节：主节奏
        (36, 0.25, 'ff'),    # Bass drum
        (36, 0.25, 'ff'),    # Bass drum
        (38, 0.5, 'f'),      # Snare
        (42, 0.25, 'mf'),    # Closed hi-hat
        (42, 0.25, 'mf'),    # Closed hi-hat
        
        # 第三小节：变奏
        (36, 0.25, 'ff'),    # Bass drum
        (42, 0.25, 'mp'),    # Closed hi-hat
        (38, 0.5, 'f'),      # Snare
        (46, 0.5, 'f'),      # Open hi-hat
        
        # 第四小节：过渡
        (47, 0.25, 'f'),     # Low-mid tom
        (48, 0.25, 'f'),     # High-mid tom
        (50, 0.25, 'f'),     # High tom
        (49, 0.25, 'ff'),    # Crash cymbal
    ]
    
    # 重复节奏型以匹配吉他部分
    for _ in range(6):
        for pitch, duration, dynamic in beat_pattern:
            n = note.Unpitched(pitch)  # 直接使用 Unpitched 类型
            n.duration.quarterLength = duration
            n.volume.velocity = get_velocity(dynamic)
            n.storedInstrument = instrument.TomTom()
            drum_part.append(n)
    
    os.makedirs(output_dir, exist_ok=True)
    midi_file = os.path.join(output_dir, 'metal_drums.mid')
    drum_part.write('midi', fp=midi_file)
    print(f"金属鼓组MIDI文件已生成: {midi_file}")
    return drum_part


def create_metal_bass(output_dir):
    """创建金属摇滚贝斯声部"""
    bass_part = stream.Part()
    
    # 使用电贝斯音色
    bass_part.append(instrument.ElectricBass())
    bass_part.append(tempo.MetronomeMark(number=150))
    
    # 创建贝斯线，主要跟随主音吉他的和弦进行
    bass_pattern = [
        # 基础节奏，保持稳定的八分音符
        ('E2', 0.5, 'mp', 'normal'),    # 根音
        ('E2', 0.5, 'mp', 'normal'),
        ('B2', 0.5, 'mp', 'normal'),    # 五度音
        ('E2', 0.5, 'mp', 'normal'),
        
        # 和弦变化处的走向
        ('A2', 0.5, 'mp', 'normal'),
        ('E2', 0.5, 'mp', 'normal'),
        ('G2', 0.5, 'mp', 'normal'),
        ('E2', 0.5, 'mp', 'normal'),
        
        # 过渡段落，稍微活跃一点
        ('E2', 0.25, 'mf', 'normal'),
        ('G2', 0.25, 'mf', 'normal'),
        ('A2', 0.5, 'mp', 'normal'),
        ('E2', 1.0, 'mp', 'normal'),
    ]
    
    # 重复节奏型以匹配吉他和鼓的长度
    for _ in range(6):
        for pitch, duration, dynamic, articulation in bass_pattern:
            n = note.Note(pitch)
            n.duration.quarterLength = duration
            # 保持较低的音量
            n.volume.velocity = get_velocity(dynamic) - 2  # 比正常力度低 2
            
            # 添加简单的表情
            if articulation == 'accent':
                n.articulations = [articulations.Accent()]
            
            bass_part.append(n)
    
    os.makedirs(output_dir, exist_ok=True)
    midi_file = os.path.join(output_dir, 'metal_bass.mid')
    bass_part.write('midi', fp=midi_file)
    print(f"金属贝斯MIDI文件已生成: {midi_file}")
    return bass_part


def create_rhythm_guitar(output_dir):
    """创建金属摇滚副音吉他声部（节奏吉他）"""
    rhythm_part = stream.Part()
    
    # 使用电吉他音色
    rhythm_part.append(instrument.ElectricGuitar())
    rhythm_part.append(tempo.MetronomeMark(number=150))
    
    # 扩展节奏型，创建更长的小节
    rhythm_pattern = [
        # 第一小节：强力和弦基础
        (('E3', 'B3', 'E4'), 2.0, 'f', 'accent'),    # 延长的E5和弦
        (('E3', 'B3', 'E4'), 2.0, 'mf', 'normal'),   
        
        # 第二小节：切分音节奏
        (('A3', 'E4', 'A4'), 1.5, 'f', 'accent'),    # 延长的A5和弦
        (('G3', 'D4', 'G4'), 1.0, 'mf', 'normal'),   # 延长的G5和弦
        (('E3', 'B3', 'E4'), 1.5, 'f', 'accent'),    # 回到E5
        
        # 第三小节：扫弦和休止的结合
        (('E3', 'B3', 'E4'), 1.0, 'f', 'tremolo'),   
        (None, 0.5, 'p', 'normal'),                   # 休止符
        (('A3', 'E4', 'A4'), 1.0, 'f', 'tremolo'),
        (None, 0.5, 'p', 'normal'),                   # 休止符
        (('G3', 'D4', 'G4'), 1.0, 'mf', 'normal'),
        
        # 第四小节：强调段落
        (('E3', 'B3', 'E4'), 0.5, 'ff', 'accent'),
        (None, 0.5, 'p', 'normal'),                   
        (('E3', 'B3', 'E4'), 1.0, 'f', 'accent'),
        (('A3', 'E4', 'A4'), 1.0, 'f', 'normal'),
        (('E3', 'B3', 'E4'), 1.0, 'ff', 'accent'),
    ]
    
    # 增加重复次数以达到约30秒
    for _ in range(3):  # 减少重复次数，因为每个模式更长了
        for pitches, duration, dynamic, effect in rhythm_pattern:
            if pitches is None:
                n = note.Rest()
            else:
                n = chord.Chord(pitches)
                n.volume.velocity = get_velocity(dynamic)
                
                if effect == 'accent':
                    n.articulations = [articulations.Accent()]
                elif effect == 'tremolo':
                    n.expressions = [expressions.Tremolo()]
            
            n.duration.quarterLength = duration
            rhythm_part.append(n)
    
    os.makedirs(output_dir, exist_ok=True)
    midi_file = os.path.join(output_dir, 'metal_rhythm_guitar.mid')
    rhythm_part.write('midi', fp=midi_file)
    print(f"金属节奏吉他MIDI文件已生成: {midi_file}")
    return rhythm_part


def create_guitar_solo(output_dir):
    """为副歌部分生成30秒的吉他独奏"""
    solo_part = stream.Part()

    # 使用失真电吉他音色
    solo_part.append(instrument.ElectricGuitar())
    solo_part.append(tempo.MetronomeMark(number=150))

    # 定义吉他独奏段落，包含丰富的技巧
    solo = [
        # 开场：扫弦和延长音
        (('E4', 'G4', 'B4', 'E5'), 1.0, 'ff', 'tremolo'),  # 扫弦
        ('E5', 0.5, 'f', 'slide'),                        # 滑音
        ('G5', 0.5, 'f', 'normal'),                       # 单音
        ('A5', 1.0, 'ff', 'bend'),                        # 弯音
        ('E6', 1.0, 'ff', 'trill'),                       # 颤音

        # 中段：快速音阶和技巧结合
        ('E5', 0.25, 'f', 'normal'),
        ('F5', 0.25, 'f', 'slide'),
        ('G5', 0.25, 'f', 'normal'),
        ('A5', 0.25, 'f', 'normal'),
        ('B5', 0.25, 'ff', 'bend'),
        ('C6', 0.25, 'ff', 'trill'),
        ('D6', 0.25, 'f', 'normal'),
        ('E6', 0.25, 'ff', 'slide'),

        # 高音区延长音和技巧
        ('E6', 1.0, 'ff', 'bend'),
        ('D6', 1.0, 'f', 'tremolo'),
        ('C6', 1.0, 'f', 'normal'),
        ('B5', 1.0, 'mf', 'normal'),

        # 快速下行音阶
        ('A5', 0.25, 'f', 'normal'),
        ('G5', 0.25, 'f', 'normal'),
        ('F5', 0.25, 'f', 'normal'),
        ('E5', 0.25, 'f', 'normal'),

        # 结束段落：扫弦和延长音
        (('E4', 'G4', 'B4', 'E5'), 1.0, 'ff', 'tremolo'),
        (('D4', 'F4', 'A4', 'D5'), 1.0, 'f', 'tremolo'),
        (('C4', 'E4', 'G4', 'C5'), 1.0, 'f', 'tremolo'),
        (('B3', 'D4', 'F4', 'B4'), 1.0, 'mf', 'tremolo'),
        ('E5', 2.0, 'ff', 'trill'),
        ('D5', 2.0, 'f', 'normal'),
        ('C5', 2.0, 'f', 'normal'),
        ('E4', 3.0, 'ff', 'accent'),  # 延长结束音

        # 新增部分：快速上行音阶和技巧结合
        ('E5', 0.25, 'f', 'normal'),
        ('F5', 0.25, 'f', 'slide'),
        ('G5', 0.25, 'f', 'normal'),
        ('A5', 0.25, 'f', 'normal'),
        ('B5', 0.25, 'ff', 'bend'),
        ('C6', 0.25, 'ff', 'trill'),
        ('D6', 0.25, 'f', 'normal'),
        ('E6', 0.25, 'ff', 'slide'),

        # 高音区延长音和颤音
        ('E6', 1.0, 'ff', 'bend'),
        ('F6', 1.0, 'f', 'tremolo'),
        ('G6', 1.0, 'f', 'normal'),
        ('A6', 1.0, 'mf', 'normal'),

        # 扫弦和快速下行音阶
        (('E4', 'G4', 'B4', 'E5'), 0.5, 'ff', 'tremolo'),
        ('E5', 0.25, 'f', 'normal'),
        ('D5', 0.25, 'f', 'normal'),
        ('C5', 0.25, 'f', 'normal'),
        ('B4', 0.25, 'f', 'normal'),

        # 结束段落：延长音和技巧结合
        ('A5', 1.0, 'ff', 'bend'),
        ('G5', 1.0, 'f', 'trill'),
        ('F5', 1.0, 'f', 'normal'),
        ('E5', 2.0, 'ff', 'accent'),
        (('E4', 'G4', 'B4', 'E5'), 2.0, 'ff', 'tremolo'),
        ('E5', 3.0, 'fff', 'trill'),  # 强烈的颤音结束
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
        if effect == 'accent':
            n.articulations = [articulations.Accent()]
        elif effect == 'slide':
            n.expressions = [expressions.Mordent()]
        elif effect == 'bend':
            n.volume = volume.Volume(velocity=110)
        elif effect == 'tremolo':
            n.expressions = [expressions.Tremolo()]
        elif effect == 'trill':
            n.expressions = [expressions.Trill()]

        solo_part.append(n)

    os.makedirs(output_dir, exist_ok=True)
    midi_file = os.path.join(output_dir, 'metal_guitar_solo.mid')
    solo_part.write('midi', fp=midi_file)
    print(f"金属吉他独奏MIDI文件已生成: {midi_file}")
    return solo_part


def create_drum_solo(output_dir):
    """为吉他独奏后的过渡段生成架子鼓独奏"""
    drum_solo_part = stream.Part()
    
    # 使用标准架子鼓音色
    drum_solo_part.append(instrument.TomTom())
    drum_solo_part.append(tempo.MetronomeMark(number=150))
    
    # 定义架子鼓独奏段落
    drum_solo = [
        # 开场：强力镲片和底鼓
        (49, 1.0, 'fff'),  # Crash cymbal
        (36, 0.5, 'ff'),   # Bass drum
        (38, 0.5, 'f'),    # Snare drum
        
        # 主节奏：底鼓和军鼓交替
        (36, 0.25, 'ff'),  # Bass drum
        (38, 0.25, 'f'),   # Snare drum
        (36, 0.25, 'ff'),  # Bass drum
        (38, 0.25, 'f'),   # Snare drum
        
        # 镲片和底鼓结合
        (42, 0.25, 'mf'),  # Closed hi-hat
        (36, 0.25, 'ff'),  # Bass drum
        (42, 0.25, 'mf'),  # Closed hi-hat
        (36, 0.25, 'ff'),  # Bass drum
        
        # 过渡：快速军鼓和镲片
        (38, 0.25, 'f'),   # Snare drum
        (49, 0.25, 'fff'), # Crash cymbal
        (38, 0.25, 'f'),   # Snare drum
        (49, 0.25, 'fff'), # Crash cymbal
        
        # 结束：低音鼓和镲片的强力结尾
        (36, 0.5, 'ff'),   # Bass drum
        (49, 1.0, 'fff'),  # Crash cymbal
        (36, 0.5, 'ff'),   # Bass drum
        (49, 1.0, 'fff'),  # Crash cymbal
        # 新增部分：快速底鼓和镲片结合
        (36, 0.25, 'ff'),   # Bass drum
        (42, 0.25, 'mf'),   # Closed hi-hat
        (36, 0.25, 'ff'),   # Bass drum
        (42, 0.25, 'mf'),   # Closed hi-hat
        (36, 0.25, 'ff'),   # Bass drum
        (49, 0.5, 'fff'),   # Crash cymbal
        
        # 军鼓和低音鼓交替
        (38, 0.25, 'f'),    # Snare drum
        (36, 0.25, 'ff'),   # Bass drum
        (38, 0.25, 'f'),    # Snare drum
        (36, 0.25, 'ff'),   # Bass drum
        (38, 0.25, 'f'),    # Snare drum
        (49, 0.5, 'fff'),   # Crash cymbal
        
        # 镲片和低音鼓的快速节奏
        (42, 0.25, 'mf'),   # Closed hi-hat
        (36, 0.25, 'ff'),   # Bass drum
        (42, 0.25, 'mf'),   # Closed hi-hat
        (36, 0.25, 'ff'),   # Bass drum
        (42, 0.25, 'mf'),   # Closed hi-hat
        (49, 0.5, 'fff'),   # Crash cymbal
        
        # 结束段落：强力的低音鼓和镲片
        (36, 0.5, 'ff'),    # Bass drum
        (49, 1.0, 'fff'),   # Crash cymbal
        (36, 0.5, 'ff'),    # Bass drum
        (49, 1.0, 'fff'),   # Crash cymbal
        (38, 0.5, 'f'),     # Snare drum
        (49, 1.0, 'fff'),   # Crash cymbal
    ]
    
    # 将独奏片段添加到音轨中
    for pitch, duration, dynamic in drum_solo:
        n = note.Unpitched(pitch)  # 使用 Unpitched 类型表示打击乐器
        n.duration.quarterLength = duration
        n.volume.velocity = get_velocity(dynamic)
        drum_solo_part.append(n)
    
    os.makedirs(output_dir, exist_ok=True)
    midi_file = os.path.join(output_dir, 'drum_solo.mid')
    drum_solo_part.write('midi', fp=midi_file)
    print(f"架子鼓独奏MIDI文件已生成: {midi_file}")
    return drum_solo_part


if __name__ == "__main__":
    # lead_guitar_melody = create_metal_lead_guitar("output/music_metal_rock_band")
    # drums_beat = create_metal_drums("output/music_metal_rock_band")
    # bass_line = create_metal_bass("output/music_metal_rock_band")
    # rhythm_guitar_part = create_rhythm_guitar("output/music_metal_rock_band")
    # guitar_solo = create_guitar_solo("output/music_metal_rock_band")
    drum_solo = create_drum_solo("output/music_metal_rock_band")

    convert_midi_to_wav("output/music_metal_rock_band")
