package validator

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/CaueTech/celer-engine/internal/domain"
)

// Erros customizados para o orquestrador saber EXATAMENTE o que falhou
var (
	ErrInvalidJSON = errors.New("payload is not a valid JSON")
	ErrEmptyFields = errors.New("event_id and robot_id are required fields")
)

// JSONValidator é a struct concreta que vai implementar a interface domain.Validator
type JSONValidator struct{}

// NewJSONValidator cria uma nova instância do validador
func NewJSONValidator() *JSONValidator {
	return &JSONValidator{}
}

/* 
	Validate pega o []byte, faz o decoding e checa os campos obrigatórios. (v *JSONValidator) indica que esta função é um método acoplado à struct JSONValidator, é similar ao this ou self em linguagens que implementam classes.
	
*/
func (v *JSONValidator) Validate(rawPayload []byte) (*domain.Event, error) {
	var event domain.Event

	// 1. Etapa de Decoding (Bytes -> Struct). O Unmarshal() já faz isso automaticamente
	err := json.Unmarshal(rawPayload, &event)
	if err != nil {
		// Se o JSON estiver quebrado (sintaxe errada), retorna o erro de sintaxe
		return nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}

	/* 
		2. Etapa de validação de regras de negócio. Se o JSON vier com campos não preenchidos, o Unmarshal, por padrão, preenche os campos com valores nulos para o respectivo tipo do campo do JSON (para strings, isso seria ""). Para verificar isso, segue a verificação a seguir.
	*/
	if event.EventID == "" || event.RobotID == "" {
		return nil, ErrEmptyFields
	}

	// 3. Sucesso! Retorna o ponteiro do evento preenchido e erro nulo
	return &event, nil
}
