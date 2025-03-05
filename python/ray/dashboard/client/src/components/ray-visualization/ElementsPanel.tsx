import React, { useState, useEffect } from 'react';
import './ElementsPanel.css';

type GraphData = {
  actors: Array<{
    id: string;
    name: string;
    language: string;
    devices?: string[];
  }>;
  methods: Array<{
    id: string;
    name: string;
    actorId: string;
    language: string;
  }>;
  functions: Array<{
    id: string;
    name: string;
    language: string;
  }>;
};

type ElementsPanelProps = {
  onElementSelect: (element: any) => void;
  selectedElementId: string | null;
  graphData: GraphData;
}

const ElementsPanel = ({ onElementSelect, selectedElementId, graphData }: ElementsPanelProps) => {
  const [activeTab, setActiveTab] = useState('actors');
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedActors, setExpandedActors] = useState<Record<string, boolean>>({});
  
  // Filter items based on search term
  const filterItems = (items: any[]) => {
    if (!searchTerm) return items;
    return items.filter(item => 
      item.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      item.id.toLowerCase().includes(searchTerm.toLowerCase())
    );
  };
  
  // Get methods for a specific actor
  const getActorMethods = (actorId: string) => {
    const methods = graphData.methods.filter(method => method.actorId === actorId);
    if (!searchTerm) return methods;
    
    return methods.filter(method =>
      method.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      method.id.toLowerCase().includes(searchTerm.toLowerCase())
    );
  };
  
  // Filter actors and their methods based on search term
  const filterActorsAndMethods = () => {
    if (!searchTerm) return graphData.actors;
    
    return graphData.actors.filter(actor => {
      const actorMatches = actor.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                          actor.id.toLowerCase().includes(searchTerm.toLowerCase());
      
      const actorMethods = getActorMethods(actor.id);
      const methodMatches = actorMethods.some(method => 
        method.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        method.id.toLowerCase().includes(searchTerm.toLowerCase())
      );
      
      return actorMatches || methodMatches;
    });
  };
  
  // Update expanded actors when search term changes
  useEffect(() => {
    if (!searchTerm) {
      setExpandedActors({});
      return;
    }

    const actorsToExpand: Record<string, boolean> = {};
    graphData.actors.forEach(actor => {
      const methods = getActorMethods(actor.id);
      if (methods.length > 0) {
        actorsToExpand[actor.id] = true;
      }
    });
    setExpandedActors(actorsToExpand);
  }, [searchTerm]);
  
  const filteredActors = filterActorsAndMethods();
  const filteredFunctions = filterItems(graphData.functions);
  
  // Toggle expanded state for an actor
  const toggleActorExpand = (actorId: string) => {
    setExpandedActors(prev => ({
      ...prev,
      [actorId]: !prev[actorId]
    }));
  };
  
  return (
    <div className="elements-panel">
      <div className="elements-header">
        <h3>Instances</h3>
        <div className="search-container">
          <input 
            type="text" 
            placeholder="Search..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="search-input"
          />
        </div>
      </div>
      
      <div className="tab-container">
        <div 
          className={`tab ${activeTab === 'actors' ? 'active' : ''}`}
          onClick={() => setActiveTab('actors')}
        >
          Actors ({graphData.actors.length})
        </div>
        <div 
          className={`tab ${activeTab === 'functions' ? 'active' : ''}`}
          onClick={() => setActiveTab('functions')}
        >
          Functions ({graphData.functions.length})
        </div>
      </div>
      
      <div className="elements-table-container">
        {activeTab === 'actors' && (
          <table className="elements-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Language</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {filteredActors.map(actor => (
                <React.Fragment key={actor.id}>
                  <tr className={`actor-row ${actor.id === selectedElementId ? 'selected' : ''}`}>
                      <button 
                        className={`expand-button ${expandedActors[actor.id] ? 'expanded' : ''}`}
                        onClick={() => toggleActorExpand(actor.id)}
                      >
                        {expandedActors[actor.id] ? '−' : '+'}
                      </button>
 
                    <div onClick={() => onElementSelect({ ...actor, type: 'actor' })}>
                      {actor.name}
                    </div>
                 </tr>
                  {expandedActors[actor.id] && (
                    <tr className="methods-container">
                      <td colSpan={3}>
                        <div className="actor-methods">
                          <h4>Methods</h4>
                          <table className="methods-table">
                            <tbody>
                              {getActorMethods(actor.id).map(method => (
                                <tr 
                                  key={method.id}
                                  className={method.id === selectedElementId ? 'selected' : ''}
                                  onClick={() => onElementSelect({ ...method, type: 'method' })}
                                >
                                  <td>{method.name}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        )}
        
        {activeTab === 'functions' && (
          <table className="elements-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Language</th>
              </tr>
            </thead>
            <tbody>
              {filteredFunctions.map(func => (
                <tr 
                  key={func.id}
                  className={func.id === selectedElementId ? 'selected' : ''}
                  onClick={() => onElementSelect({ ...func, type: 'function' })}
                >
                  <td>{func.name}</td>
                  <td>{func.language}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
};

export default ElementsPanel; 